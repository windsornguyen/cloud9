use std::path::{Path, PathBuf};

use fs_err::{self as fs, File, OpenOptions};

use crate::error::{Corruption, Result, WalError};
use crate::format::{self, ReadOne};
use crate::record::{Lsn, StoredRecord};

const SEGMENT_SUFFIX: &str = "wal";
const LOCK_FILE: &str = "LOCK";

pub(crate) fn recover(dir: &Path, segment_size: u64) -> Result<(u64, u64)> {
    let mut ids = segment_ids(dir)?;
    if ids.is_empty() {
        let segment_id = 0;
        File::create(segment_path(dir, segment_id))
            .map_err(|source| WalError::io(segment_path(dir, segment_id), source))?;
        sync_dir(dir)?;
        ids.push(segment_id);
    }

    for (position, segment_id) in ids.iter().copied().enumerate() {
        match valid_len(dir, segment_id, segment_size)? {
            SegmentScan::Clean(len) => {
                if position + 1 == ids.len() {
                    return Ok((segment_id, len));
                }
            }
            SegmentScan::IncompleteTail(len) => {
                if position + 1 != ids.len() {
                    return Err(WalError::CorruptRecord {
                        lsn: Lsn { segment_id, offset: len },
                        reason: Corruption::IncompleteRecord,
                    });
                }
                truncate_segment(dir, segment_id, len)?;
                return Ok((segment_id, len));
            }
        }
    }
    let last = ids.last().copied().ok_or(WalError::SegmentIdExhausted)?;
    Ok((last, segment_len(dir, last)?))
}

pub(crate) struct SegmentReader {
    path: PathBuf,
    segment_id: u64,
    segment_size: u64,
    physical_len: u64,
    offset: u64,
    file: File,
}

impl SegmentReader {
    pub(crate) fn open(dir: &Path, segment_id: u64, segment_size: u64) -> Result<Self> {
        let path = segment_path(dir, segment_id);
        let file = File::open(&path).map_err(|source| WalError::io(path.clone(), source))?;
        let physical_len =
            file.metadata().map_err(|source| WalError::io(path.clone(), source))?.len();
        Ok(Self { path, segment_id, segment_size, physical_len, offset: 0, file })
    }

    pub(crate) fn next_record(&mut self) -> Result<Option<StoredRecord>> {
        let lsn = Lsn { segment_id: self.segment_id, offset: self.offset };
        match format::read_one(
            &self.path,
            &mut self.file,
            lsn,
            self.segment_size,
            self.physical_len,
        )? {
            ReadOne::Record { record, next_offset } => {
                self.offset = next_offset;
                Ok(Some(StoredRecord { lsn, record }))
            }
            ReadOne::Eof => Ok(None),
            ReadOne::Incomplete => {
                Err(WalError::CorruptRecord { lsn, reason: Corruption::IncompleteRecord })
            }
        }
    }
}

pub(crate) fn segment_ids(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir).map_err(|source| WalError::io(dir.to_path_buf(), source))? {
        let entry = entry.map_err(|source| WalError::io(dir.to_path_buf(), source))?;
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) == Some(SEGMENT_SUFFIX) {
            ids.push(segment_id(&path)?);
        }
    }
    ids.sort_unstable();
    for (expected, found) in ids.iter().copied().enumerate() {
        let expected = u64::try_from(expected).map_err(|_| WalError::SegmentIdExhausted)?;
        if found != expected {
            return Err(WalError::MissingSegment { expected, found });
        }
    }
    Ok(ids)
}

pub(crate) fn segment_path(dir: &Path, segment_id: u64) -> PathBuf {
    dir.join(format!("{segment_id:020}.wal"))
}

pub(crate) fn open_segment(dir: &Path, segment_id: u64) -> Result<File> {
    let path = segment_path(dir, segment_id);
    OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(&path)
        .map_err(|source| WalError::io(path, source))
}

pub(crate) fn create_dir_all(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut cursor = path;
    while !cursor.try_exists().map_err(|source| WalError::io(cursor.to_path_buf(), source))? {
        missing.push(cursor.to_path_buf());
        cursor = cursor.parent().ok_or(WalError::SegmentIdExhausted)?;
    }
    fs::create_dir_all(path).map_err(|source| WalError::io(path.to_path_buf(), source))?;
    for created in missing.iter().rev() {
        let parent = created.parent().ok_or(WalError::SegmentIdExhausted)?;
        sync_dir(parent)?;
    }
    Ok(())
}

pub(crate) fn lock(dir: &Path) -> Result<File> {
    let path = dir.join(LOCK_FILE);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|source| WalError::io(path.clone(), source))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(WalError::Locked { path }),
        Err(std::fs::TryLockError::Error(source)) => Err(WalError::io(path, source)),
    }
}

pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|file| file.sync_all())
        .map_err(|source| WalError::io(dir.to_path_buf(), source))
}

enum SegmentScan {
    Clean(u64),
    IncompleteTail(u64),
}

fn valid_len(dir: &Path, segment_id: u64, segment_size: u64) -> Result<SegmentScan> {
    let path = segment_path(dir, segment_id);
    let mut file = File::open(&path).map_err(|source| WalError::io(path.clone(), source))?;
    let physical_len = file.metadata().map_err(|source| WalError::io(path.clone(), source))?.len();
    let mut offset = 0;
    loop {
        match format::read_one(
            &path,
            &mut file,
            Lsn { segment_id, offset },
            segment_size,
            physical_len,
        )? {
            ReadOne::Record { next_offset, .. } => offset = next_offset,
            ReadOne::Eof => return Ok(SegmentScan::Clean(offset)),
            ReadOne::Incomplete => return Ok(SegmentScan::IncompleteTail(offset)),
        }
    }
}

fn segment_id(path: &Path) -> Result<u64> {
    let Some(stem) = path.file_stem().and_then(std::ffi::OsStr::to_str) else {
        return Err(WalError::BadSegmentName { path: path.to_path_buf() });
    };
    stem.parse::<u64>().map_err(|_| WalError::BadSegmentName { path: path.to_path_buf() })
}

fn segment_len(dir: &Path, segment_id: u64) -> Result<u64> {
    let path = segment_path(dir, segment_id);
    fs::metadata(&path).map_err(|source| WalError::io(path, source)).map(|meta| meta.len())
}

pub(crate) fn total_len(dir: &Path) -> Result<u64> {
    segment_ids(dir)?.into_iter().try_fold(0_u64, |total, segment_id| {
        total
            .checked_add(segment_len(dir, segment_id)?)
            .ok_or(WalError::WalFull { len: u64::MAX, max_size: u64::MAX })
    })
}

fn truncate_segment(dir: &Path, segment_id: u64, len: u64) -> Result<()> {
    let path = segment_path(dir, segment_id);
    let file = OpenOptions::new()
        .write(true)
        .open(&path)
        .map_err(|source| WalError::io(path.clone(), source))?;
    file.set_len(len).map_err(|source| WalError::io(path.clone(), source))?;
    file.sync_all().map_err(|source| WalError::io(path, source))
}

use std::path::{Path, PathBuf};

use fs_err::{self as fs, File, OpenOptions};

use crate::error::{Corruption, Result, WalError};
use crate::format::{self, ReadOne};
use crate::record::{Lsn, StoredRecord};

const SEGMENT_SUFFIX: &str = "wal";

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
                truncate_segment(dir, segment_id, len)?;
                remove_segments_after(dir, &ids, position)?;
                sync_dir(dir)?;
                return Ok((segment_id, len));
            }
        }
    }
    let last = ids.last().copied().ok_or(WalError::SegmentIdExhausted)?;
    Ok((last, segment_len(dir, last)?))
}

pub(crate) fn read_segment(
    dir: &Path,
    segment_id: u64,
    segment_size: u64,
    records: &mut Vec<StoredRecord>,
) -> Result<()> {
    let path = segment_path(dir, segment_id);
    let mut file = File::open(&path).map_err(|source| WalError::io(path.clone(), source))?;
    let mut offset = 0;
    loop {
        match format::read_one(&path, &mut file, Lsn { segment_id, offset }, segment_size)? {
            ReadOne::Record { record, next_offset } => {
                records.push(StoredRecord { lsn: Lsn { segment_id, offset }, record });
                offset = next_offset;
            }
            ReadOne::Eof => return Ok(()),
            ReadOne::Incomplete => {
                return Err(WalError::CorruptRecord {
                    lsn: Lsn { segment_id, offset },
                    reason: Corruption::IncompleteRecord,
                });
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
    let mut offset = 0;
    loop {
        match format::read_one(&path, &mut file, Lsn { segment_id, offset }, segment_size)? {
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

fn truncate_segment(dir: &Path, segment_id: u64, len: u64) -> Result<()> {
    let path = segment_path(dir, segment_id);
    let file = OpenOptions::new()
        .write(true)
        .open(&path)
        .map_err(|source| WalError::io(path.clone(), source))?;
    file.set_len(len).map_err(|source| WalError::io(path.clone(), source))?;
    file.sync_all().map_err(|source| WalError::io(path, source))
}

fn remove_segments_after(dir: &Path, ids: &[u64], position: usize) -> Result<()> {
    for segment_id in &ids[position + 1..] {
        let path = segment_path(dir, *segment_id);
        fs::remove_file(&path).map_err(|source| WalError::io(path, source))?;
    }
    Ok(())
}

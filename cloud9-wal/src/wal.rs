use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs_err::{self as fs, File};

use crate::error::{Result, WalError};
use crate::format;
use crate::record::{Lsn, RecordKind, StoredRecord};
use crate::segment;

const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalOptions {
    pub segment_size: u64,
    pub sync_on_append: bool,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self { segment_size: DEFAULT_SEGMENT_SIZE, sync_on_append: false }
    }
}

/// Append-only segmented WAL.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    options: WalOptions,
    active_id: u64,
    active_len: u64,
    active: File,
}

impl Wal {
    /// Open or create a WAL directory.
    pub fn open(dir: impl AsRef<Path>, options: WalOptions) -> Result<Self> {
        validate_options(&options)?;
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|source| WalError::io(dir.clone(), source))?;
        let (active_id, active_len) = segment::recover(&dir, options.segment_size)?;
        let active = segment::open_segment(&dir, active_id)?;
        let mut wal = Self { dir, options, active_id, active_len, active };
        wal.active.seek(SeekFrom::Start(active_len)).map_err(|source| {
            WalError::io(segment::segment_path(&wal.dir, wal.active_id), source)
        })?;
        Ok(wal)
    }

    /// Append a record and return its LSN.
    pub fn append(&mut self, kind: RecordKind, payload: impl AsRef<[u8]>) -> Result<Lsn> {
        let encoded = format::encode_record(kind, payload.as_ref())?;
        self.rotate_if_needed(encoded.len())?;
        let lsn = Lsn { segment_id: self.active_id, offset: self.active_len };
        self.active.write_all(&encoded).map_err(|source| {
            WalError::io(segment::segment_path(&self.dir, self.active_id), source)
        })?;
        self.active_len = checked_add_len(self.active_len, encoded.len())?;
        if self.options.sync_on_append {
            self.sync()?;
        }
        Ok(lsn)
    }

    /// Force pending WAL bytes to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.active.sync_all().map_err(|source| {
            WalError::io(segment::segment_path(&self.dir, self.active_id), source)
        })
    }

    /// Read every valid record in LSN order.
    pub fn records(&self) -> Result<Vec<StoredRecord>> {
        let mut records = Vec::new();
        for segment_id in segment::segment_ids(&self.dir)? {
            segment::read_segment(&self.dir, segment_id, self.options.segment_size, &mut records)?;
        }
        Ok(records)
    }

    fn rotate_if_needed(&mut self, record_len: usize) -> Result<()> {
        let record_len = u64::try_from(record_len).map_err(|_| WalError::RecordTooLarge {
            len: u64::MAX,
            segment_size: self.options.segment_size,
        })?;
        if record_len > self.options.segment_size {
            return Err(WalError::RecordTooLarge {
                len: record_len,
                segment_size: self.options.segment_size,
            });
        }
        let next_len = self.active_len.checked_add(record_len).ok_or(WalError::RecordTooLarge {
            len: u64::MAX,
            segment_size: self.options.segment_size,
        })?;
        if self.active_len != 0 && next_len > self.options.segment_size {
            self.sync()?;
            self.active_id = self.active_id.checked_add(1).ok_or(WalError::SegmentIdExhausted)?;
            self.active = segment::open_segment(&self.dir, self.active_id)?;
            self.active_len = 0;
            segment::sync_dir(&self.dir)?;
        }
        Ok(())
    }
}

fn validate_options(options: &WalOptions) -> Result<()> {
    if options.segment_size < format::HEADER_LEN_U64 {
        Err(WalError::SegmentTooSmall {
            segment_size: options.segment_size,
            header_len: format::HEADER_LEN,
        })
    } else {
        Ok(())
    }
}

fn checked_add_len(offset: u64, len: usize) -> Result<u64> {
    offset
        .checked_add(
            u64::try_from(len)
                .map_err(|_| WalError::RecordTooLarge { len: u64::MAX, segment_size: u64::MAX })?,
        )
        .ok_or(WalError::RecordTooLarge { len: u64::MAX, segment_size: u64::MAX })
}

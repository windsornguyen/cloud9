use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs_err::File;

use crate::error::{Result, WalError};
use crate::format;
use crate::record::{Lsn, RecordKind, StoredRecord};
use crate::segment::{self, SegmentReader};

const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalOptions {
    pub segment_size: u64,
    pub max_size: u64,
    pub sync_on_append: bool,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            segment_size: DEFAULT_SEGMENT_SIZE,
            max_size: DEFAULT_MAX_SIZE,
            sync_on_append: false,
        }
    }
}

/// Append-only segmented WAL.
#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    options: WalOptions,
    _lock: File,
    active_id: u64,
    active_len: u64,
    total_len: u64,
    active: File,
    poisoned: bool,
}

/// Streaming WAL records in LSN order.
pub struct Records<'a> {
    dir: &'a Path,
    segment_size: u64,
    segment_ids: std::vec::IntoIter<u64>,
    segment: Option<SegmentReader>,
    done: bool,
}

impl Iterator for Records<'_> {
    type Item = Result<StoredRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if let Some(segment) = &mut self.segment {
                match segment.next_record() {
                    Ok(Some(record)) => return Some(Ok(record)),
                    Ok(None) => self.segment = None,
                    Err(error) => {
                        self.done = true;
                        return Some(Err(error));
                    }
                }
            } else if let Some(segment_id) = self.segment_ids.next() {
                match SegmentReader::open(self.dir, segment_id, self.segment_size) {
                    Ok(segment) => self.segment = Some(segment),
                    Err(error) => {
                        self.done = true;
                        return Some(Err(error));
                    }
                }
            } else {
                self.done = true;
                return None;
            }
        }
    }
}

impl Wal {
    /// Open or create a WAL directory.
    pub fn open(dir: impl AsRef<Path>, options: WalOptions) -> Result<Self> {
        validate_options(&options)?;
        let dir = absolute_path(dir.as_ref())?;
        segment::create_dir_all(&dir)?;
        let lock = segment::lock(&dir)?;
        let (active_id, active_len) = segment::recover(&dir, options.segment_size)?;
        let total_len = segment::total_len(&dir)?;
        if total_len > options.max_size {
            return Err(WalError::WalFull { len: total_len, max_size: options.max_size });
        }
        let active = segment::open_segment(&dir, active_id)?;
        let mut wal = Self {
            dir,
            options,
            _lock: lock,
            active_id,
            active_len,
            total_len,
            active,
            poisoned: false,
        };
        wal.active.seek(SeekFrom::Start(active_len)).map_err(|source| {
            WalError::io(segment::segment_path(&wal.dir, wal.active_id), source)
        })?;
        Ok(wal)
    }

    /// Append a record and return its LSN.
    pub fn append(&mut self, kind: RecordKind, payload: impl AsRef<[u8]>) -> Result<Lsn> {
        self.ensure_healthy()?;
        let payload = payload.as_ref();
        let record_len = format::encoded_len(payload.len())?;
        let next_total = checked_add_len(self.total_len, record_len)?;
        if next_total > self.options.max_size {
            return Err(WalError::WalFull { len: next_total, max_size: self.options.max_size });
        }
        self.rotate_if_needed(record_len)?;
        let encoded = format::encode_record(kind, payload)?;
        let lsn = Lsn { segment_id: self.active_id, offset: self.active_len };
        let next_len = checked_add_len(self.active_len, encoded.len())?;
        write_or_poison(&mut self.active, &encoded, &mut self.poisoned).map_err(|source| {
            WalError::io(segment::segment_path(&self.dir, self.active_id), source)
        })?;
        self.active_len = next_len;
        self.total_len = next_total;
        if self.options.sync_on_append {
            self.sync()?;
        }
        Ok(lsn)
    }

    /// Force pending WAL bytes to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        if let Err(source) = self.active.sync_all() {
            self.poisoned = true;
            return Err(WalError::io(segment::segment_path(&self.dir, self.active_id), source));
        }
        Ok(())
    }

    /// Stream every valid record in LSN order.
    pub fn records(&self) -> Result<Records<'_>> {
        self.ensure_healthy()?;
        Ok(Records {
            dir: &self.dir,
            segment_size: self.options.segment_size,
            segment_ids: segment::segment_ids(&self.dir)?.into_iter(),
            segment: None,
            done: false,
        })
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
            let active_id = self.active_id.checked_add(1).ok_or(WalError::SegmentIdExhausted)?;
            let active = segment::open_segment(&self.dir, active_id)?;
            segment::sync_dir(&self.dir)?;
            self.active_id = active_id;
            self.active = active;
            self.active_len = 0;
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned { Err(WalError::Poisoned) } else { Ok(()) }
    }
}

fn write_or_poison(
    writer: &mut impl Write,
    bytes: &[u8],
    poisoned: &mut bool,
) -> std::io::Result<()> {
    if let Err(error) = writer.write_all(bytes) {
        *poisoned = true;
        return Err(error);
    }
    Ok(())
}

fn validate_options(options: &WalOptions) -> Result<()> {
    if options.segment_size < format::HEADER_LEN_U64 || options.max_size < format::HEADER_LEN_U64 {
        Err(WalError::SegmentTooSmall {
            segment_size: options.segment_size.min(options.max_size),
            header_len: format::HEADER_LEN,
        })
    } else {
        Ok(())
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .map_err(|source| WalError::io(PathBuf::from("."), source))
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

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::write_or_poison;

    struct PartialWriter(bool);

    impl Write for PartialWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.0 {
                Err(io::Error::other("disk write failed"))
            } else {
                self.0 = true;
                Ok(bytes.len().min(1))
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn invariant_partial_write_invalidates_handle() {
        let mut writer = PartialWriter(false);
        let mut poisoned = false;

        assert!(write_or_poison(&mut writer, &[1, 2], &mut poisoned).is_err());
        assert!(poisoned);
    }
}

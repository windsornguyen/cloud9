use std::io;
use std::path::PathBuf;

use thiserror::Error;

use crate::record::Lsn;

/// WAL failures.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("I/O error at `{path}`")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("record kind zero is reserved")]
    ReservedRecordKind,
    #[error("segment size {segment_size} is smaller than record header {header_len}")]
    SegmentTooSmall { segment_size: u64, header_len: usize },
    #[error("record length {len} exceeds segment size {segment_size}")]
    RecordTooLarge { len: u64, segment_size: u64 },
    #[error("WAL size {len} exceeds configured maximum {max_size}")]
    WalFull { len: u64, max_size: u64 },
    #[error("payload length {len} exceeds u32::MAX")]
    PayloadTooLarge { len: usize },
    #[error("segment id exhausted")]
    SegmentIdExhausted,
    #[error("corrupt WAL record at segment {lsn:?}: {reason}")]
    CorruptRecord { lsn: Lsn, reason: Corruption },
    #[error("malformed WAL segment filename `{path}`")]
    BadSegmentName { path: PathBuf },
    #[error("missing WAL segment {expected:020}.wal before {found:020}.wal")]
    MissingSegment { expected: u64, found: u64 },
    #[error("WAL directory `{path}` already has a writer")]
    Locked { path: PathBuf },
    #[error("WAL handle is poisoned; reopen it to recover")]
    Poisoned,
}

impl WalError {
    pub(crate) fn io(path: PathBuf, source: io::Error) -> Self {
        Self::Io { path, source }
    }
}

/// Specific corruption detected while scanning records.
#[derive(Debug, Error)]
pub enum Corruption {
    #[error("bad magic {found:#x}")]
    BadMagic { found: u32 },
    #[error("unsupported version {found}")]
    UnsupportedVersion { found: u16 },
    #[error("reserved record kind")]
    ReservedRecordKind,
    #[error("header checksum mismatch")]
    HeaderChecksum,
    #[error("payload checksum mismatch")]
    PayloadChecksum,
    #[error("record length {len} exceeds segment size {segment_size}")]
    RecordTooLarge { len: u64, segment_size: u64 },
    #[error("segment length {len} exceeds configured size {segment_size}")]
    SegmentTooLarge { len: u64, segment_size: u64 },
    #[error("incomplete record")]
    IncompleteRecord,
}

pub type Result<T> = std::result::Result<T, WalError>;

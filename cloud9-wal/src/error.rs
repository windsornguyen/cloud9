use std::io;
use std::path::PathBuf;

use thiserror::Error;

use crate::record::Lsn;

/// WAL failures.
#[derive(Debug, Error)]
pub enum WalError {
    /// An operation on a WAL path failed.
    #[error("I/O error at `{path}`")]
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Operating-system error returned for the path.
        #[source]
        source: io::Error,
    },
    /// Record kind zero was supplied.
    #[error("record kind zero is reserved")]
    ReservedRecordKind,
    /// Configured segments cannot hold one record header.
    #[error("segment size {segment_size} is smaller than record header {header_len}")]
    SegmentTooSmall {
        /// Configured segment capacity.
        segment_size: u64,
        /// Bytes required by the fixed header.
        header_len: usize,
    },
    /// An encoded record cannot fit in one segment.
    #[error("record length {len} exceeds segment size {segment_size}")]
    RecordTooLarge {
        /// Encoded record length.
        len: u64,
        /// Configured segment capacity.
        segment_size: u64,
    },
    /// An append would exceed the configured WAL capacity.
    #[error("WAL size {len} exceeds configured maximum {max_size}")]
    WalFull {
        /// Total bytes after the rejected append.
        len: u64,
        /// Configured WAL capacity.
        max_size: u64,
    },
    /// A payload cannot be represented by the on-disk length field.
    #[error("payload length {len} exceeds u32::MAX")]
    PayloadTooLarge {
        /// Rejected payload length.
        len: usize,
    },
    /// The next segment ID cannot be represented.
    #[error("segment id exhausted")]
    SegmentIdExhausted,
    /// Recovery found a malformed durable record.
    #[error("corrupt WAL record at segment {lsn:?}: {reason}")]
    CorruptRecord {
        /// Position of the malformed record.
        lsn: Lsn,
        /// Corruption detected at the position.
        reason: Corruption,
    },
    /// A segment filename does not use the canonical numeric form.
    #[error("malformed WAL segment filename `{path}`")]
    BadSegmentName {
        /// Malformed segment path.
        path: PathBuf,
    },
    /// The ordered segment sequence contains a gap.
    #[error("missing WAL segment {expected:020}.wal before {found:020}.wal")]
    MissingSegment {
        /// Segment ID required next.
        expected: u64,
        /// Later segment ID found on disk.
        found: u64,
    },
    /// Another writer holds the directory lock.
    #[error("WAL directory `{path}` already has a writer")]
    Locked {
        /// Locked WAL directory.
        path: PathBuf,
    },
    /// A partial write invalidated the open handle.
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
    /// The record header does not start with the WAL magic number.
    #[error("bad magic {found:#x}")]
    BadMagic {
        /// Unexpected magic number.
        found: u32,
    },
    /// The record uses an unsupported format version.
    #[error("unsupported version {found}")]
    UnsupportedVersion {
        /// Unsupported on-disk version.
        found: u16,
    },
    /// The record uses kind zero.
    #[error("reserved record kind")]
    ReservedRecordKind,
    /// The record header checksum does not match its fields.
    #[error("header checksum mismatch")]
    HeaderChecksum,
    /// The payload checksum does not match its bytes.
    #[error("payload checksum mismatch")]
    PayloadChecksum,
    /// A record length exceeds the configured segment capacity.
    #[error("record length {len} exceeds segment size {segment_size}")]
    RecordTooLarge {
        /// Encoded record length from the header.
        len: u64,
        /// Configured segment capacity.
        segment_size: u64,
    },
    /// A segment file exceeds its configured capacity.
    #[error("segment length {len} exceeds configured size {segment_size}")]
    SegmentTooLarge {
        /// Segment file length.
        len: u64,
        /// Configured segment capacity.
        segment_size: u64,
    },
    /// The final record ends before its declared length.
    #[error("incomplete record")]
    IncompleteRecord,
}

/// WAL operation result.
pub type Result<T> = std::result::Result<T, WalError>;

use crate::{Result, WalError};

/// Position of a WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn {
    /// Segment containing the record.
    pub segment_id: u64,
    /// Byte offset of the record header within the segment.
    pub offset: u64,
}

/// Caller-owned record kind. Kind zero is reserved for invalid headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordKind(u16);

impl RecordKind {
    /// Create a non-zero record kind.
    pub fn new(value: u16) -> Result<Self> {
        if value == 0 { Err(WalError::ReservedRecordKind) } else { Ok(Self(value)) }
    }

    /// Return the on-disk kind value.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// One logical WAL record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Caller-defined non-zero record type.
    pub kind: RecordKind,
    /// Uninterpreted record body.
    pub payload: Vec<u8>,
}

/// A record with its log position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    /// Durable position of the record.
    pub lsn: Lsn,
    /// Decoded record contents.
    pub record: Record,
}

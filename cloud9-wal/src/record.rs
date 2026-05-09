use crate::{Result, WalError};

/// Position of a WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn {
    pub segment_id: u64,
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
    pub kind: RecordKind,
    pub payload: Vec<u8>,
}

/// A record with its log position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    pub lsn: Lsn,
    pub record: Record,
}

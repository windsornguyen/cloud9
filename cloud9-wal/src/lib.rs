#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![warn(missing_docs)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

//! Segmented write-ahead log (WAL).
//!
//! The WAL deliberately owns only the byte-durability problem:
//! 1. append one typed byte record to the active segment,
//! 2. sync the active segment when the caller asks for durability,
//! 3. recover by scanning segment files and truncating only an incomplete tail.
//!
//! Higher layers own meaning. Raft entries, hard state, logical truncation, and
//! snapshots are just payloads encoded by the caller.

mod error;
mod format;
mod record;
mod segment;
#[cfg(test)]
mod tests;
mod wal;

pub use error::{Corruption, Result, WalError};
pub use record::{Lsn, Record, RecordKind, StoredRecord};
pub use wal::{Records, Wal, WalOptions};

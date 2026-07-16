//! Durable Raft state over the Cloud9 WAL.
//!
//! Each record stores the hard state plus only the changed log suffix. Recovery
//! replays those deltas into the exact `Persistent` value consumed by Raft.

use std::path::Path;

use cloud9_raft::raft::{Entry, Persistent};
use cloud9_raft::{LogIndex, NodeId};
use cloud9_wal::{RecordKind, Wal, WalError, WalOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const RAFT_STATE_KIND: u16 = 1;

#[derive(Debug, Error)]
pub(crate) enum StoreError {
    #[error(transparent)]
    Wal(#[from] WalError),
    #[error("failed to encode Raft persistent state")]
    Encode(#[source] serde_json::Error),
    #[error("failed to decode Raft persistent state")]
    Decode(#[source] serde_json::Error),
    #[error("unexpected WAL record kind {found}")]
    UnexpectedKind { found: u16 },
    #[error("Raft WAL term regressed from {previous} to {next}")]
    TermRegression { previous: u64, next: u64 },
    #[error("Raft WAL truncation index {index} exceeds durable log end {last_index}")]
    InvalidTruncation { index: LogIndex, last_index: LogIndex },
    #[error("Raft WAL entry index {found} is not the expected index {expected}")]
    NoncontiguousEntry { expected: LogIndex, found: LogIndex },
    #[error("snapshot persistence is not implemented")]
    SnapshotUnsupported,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistentDelta {
    term: u64,
    voted_for: Option<NodeId>,
    bootstrap_config: cloud9_raft::Configuration,
    truncate_after: LogIndex,
    entries: Vec<Entry>,
}

pub(crate) struct RaftStore {
    wal: Wal,
    kind: RecordKind,
    durable: Persistent,
}

impl RaftStore {
    pub(crate) fn open(path: &Path, initial: Persistent) -> Result<Self, StoreError> {
        let wal = Wal::open(path, WalOptions::default())?;
        let kind = RecordKind::new(RAFT_STATE_KIND)?;
        let mut store = Self { wal, kind, durable: initial };
        store.recover()?;
        Ok(store)
    }

    pub(crate) fn persistent(&self) -> &Persistent {
        &self.durable
    }

    pub(crate) fn save(&mut self, current: &Persistent) -> Result<(), StoreError> {
        if current.log.snapshot_index() != self.durable.log.snapshot_index() {
            return Err(StoreError::SnapshotUnsupported);
        }
        let truncate_after = common_prefix(&self.durable, current);
        let entries = current.log.slice(truncate_after + 1, current.log.last_index()).to_vec();
        let delta = PersistentDelta {
            term: current.term,
            voted_for: current.voted_for,
            bootstrap_config: current.bootstrap_config.clone(),
            truncate_after,
            entries,
        };
        let encoded = serde_json::to_vec(&delta).map_err(StoreError::Encode)?;
        self.wal.append(self.kind, encoded)?;
        self.wal.sync()?;
        self.durable = current.clone();
        Ok(())
    }

    fn recover(&mut self) -> Result<(), StoreError> {
        for stored in self.wal.records()? {
            let stored = stored?;
            if stored.record.kind != self.kind {
                return Err(StoreError::UnexpectedKind { found: stored.record.kind.get() });
            }
            let delta =
                serde_json::from_slice(&stored.record.payload).map_err(StoreError::Decode)?;
            apply_delta(&mut self.durable, delta)?;
        }
        Ok(())
    }
}

fn common_prefix(durable: &Persistent, current: &Persistent) -> LogIndex {
    let floor = durable.log.snapshot_index().max(current.log.snapshot_index());
    let mut index = durable.log.last_index().min(current.log.last_index());
    while index > floor && durable.log.term_at(index) != current.log.term_at(index) {
        index -= 1;
    }
    index
}

fn apply_delta(state: &mut Persistent, delta: PersistentDelta) -> Result<(), StoreError> {
    if delta.term < state.term {
        return Err(StoreError::TermRegression { previous: state.term, next: delta.term });
    }
    let last_index = state.log.last_index();
    if delta.truncate_after > last_index {
        return Err(StoreError::InvalidTruncation { index: delta.truncate_after, last_index });
    }
    state.log.truncate_after(delta.truncate_after);
    for entry in delta.entries {
        let expected = state.log.last_index() + 1;
        if entry.index != expected {
            return Err(StoreError::NoncontiguousEntry { expected, found: entry.index });
        }
        state.log.append(entry);
    }
    state.term = delta.term;
    state.voted_for = delta.voted_for;
    state.bootstrap_config = delta.bootstrap_config;
    Ok(())
}

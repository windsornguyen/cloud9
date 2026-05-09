//! The replicated log.
//!
//! Raft's core data structure: an append-only sequence of entries,
//! indexed starting at 1 (index 0 is the "null" entry).
//!
//! Entries can be either state machine commands or configuration changes.
//! Configuration entries are special: they take effect when appended
//! (not when committed), per §4 of the dissertation.

use serde::{Deserialize, Serialize};

use crate::{Command, LogIndex, Term};

use super::membership::Configuration;

/// The payload of a log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EntryPayload {
    /// A command to be applied to the state machine.
    Command(Command),
    /// A configuration change (takes effect on append, not commit).
    Config(Configuration),
}

impl EntryPayload {
    /// Check if this is a configuration entry.
    pub fn is_config(&self) -> bool {
        matches!(self, Self::Config(_))
    }

    /// Get the configuration if this is a config entry.
    pub fn as_config(&self) -> Option<&Configuration> {
        match self {
            Self::Config(c) => Some(c),
            Self::Command(_) => None,
        }
    }
}

/// A single log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: EntryPayload,
}

/// The replicated log (§3.5, §5).
///
/// Invariants maintained:
/// - Entries are contiguous starting at `snapshot_index + 1`
/// - Entry at index i has `entry.index == i`
/// - Append-only except for conflict resolution (truncate suffix, §3.5)
/// - Prefix can be discarded after snapshotting (truncate prefix, §5)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Log {
    entries: Vec<Entry>,
    /// Index of the last entry included in the snapshot (0 if no snapshot).
    /// Corresponds to `lastIncludedIndex` in Figure 5.3.
    snapshot_index: LogIndex,
    /// Term of the last entry included in the snapshot.
    /// Corresponds to `lastIncludedTerm` in Figure 5.3.
    snapshot_term: Term,
    /// Latest configuration as of `snapshot_index`.
    snapshot_config: Option<Configuration>,
}

impl Log {
    // --- Snapshot metadata ---

    /// Index of the last entry included in the snapshot.
    #[inline]
    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    /// Term of the last entry included in the snapshot.
    #[inline]
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }

    /// First index with an available entry, or `snapshot_index + 1` if empty.
    #[inline]
    pub fn first_index(&self) -> LogIndex {
        self.snapshot_index + 1
    }

    // --- Basic accessors ---

    /// Index of the last entry, or `snapshot_index` if log is empty.
    #[inline]
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map_or(self.snapshot_index, |e| e.index)
    }

    /// Term of the last entry, or `snapshot_term` if log is empty.
    #[inline]
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.snapshot_term, |e| e.term)
    }

    /// Term at a given index, or 0 if index is 0 or entry is not available.
    ///
    /// Returns `snapshot_term` for `snapshot_index`, since leaders use that
    /// entry as the consistency boundary after compaction.
    /// Callers should check `snapshot_index()` to distinguish missing vs discarded.
    #[inline]
    pub fn term_at(&self, index: LogIndex) -> Term {
        if index == 0 || index < self.snapshot_index {
            0
        } else if index == self.snapshot_index {
            self.snapshot_term
        } else {
            self.to_vec_index(index).and_then(|i| self.entries.get(i)).map_or(0, |e| e.term)
        }
    }

    /// Get entry at index, or None if out of bounds or discarded.
    #[inline]
    pub fn get(&self, index: LogIndex) -> Option<&Entry> {
        if index == 0 || index <= self.snapshot_index {
            None
        } else {
            self.to_vec_index(index).and_then(|i| self.entries.get(i))
        }
    }

    // --- Mutations ---

    /// Append an entry. Must be the next sequential index.
    pub fn append(&mut self, entry: Entry) {
        debug_assert_eq!(entry.index, self.last_index() + 1);
        self.entries.push(entry);
    }

    /// Truncate log to keep only entries up to and including `index`.
    ///
    /// Cannot truncate before `snapshot_index`. If `index <= snapshot_index`,
    /// this clears all entries but preserves snapshot metadata.
    pub fn truncate_after(&mut self, index: LogIndex) {
        if index <= self.snapshot_index {
            self.entries.clear();
        } else if let Some(vec_idx) = self.to_vec_index(index) {
            self.entries.truncate(vec_idx + 1);
        }
    }

    /// Discard entries up to and including `index` after snapshotting.
    ///
    /// Updates snapshot metadata and removes entries from the log.
    /// The term should be the term of the entry at `index`.
    pub fn truncate_prefix(&mut self, index: LogIndex, term: Term) {
        if index <= self.snapshot_index {
            return; // Already discarded
        }
        let snapshot_config = self.config_at(index).cloned();

        // Remove entries up to and including index
        let entries_to_remove =
            usize::try_from(index - self.snapshot_index).unwrap_or(self.entries.len());
        if entries_to_remove >= self.entries.len() {
            self.entries.clear();
        } else {
            self.entries.drain(..entries_to_remove);
        }

        self.snapshot_index = index;
        self.snapshot_term = term;
        self.snapshot_config = snapshot_config;
    }

    /// Install snapshot metadata (for followers receiving `InstallSnapshot`).
    ///
    /// Retains entries after the snapshot if the snapshot describes a prefix
    /// of this log; otherwise discards the suffix as superseded.
    pub fn install_snapshot(&mut self, index: LogIndex, term: Term, config: Configuration) {
        if index > self.snapshot_index && self.term_at(index) == term {
            self.truncate_prefix(index, term);
        } else {
            self.entries.clear();
            self.snapshot_index = index;
            self.snapshot_term = term;
        }
        self.snapshot_config = Some(config);
    }

    /// Whether installing this snapshot would retain log entries after it.
    pub fn snapshot_matches_prefix(&self, index: LogIndex, term: Term) -> bool {
        index > self.snapshot_index && self.term_at(index) == term
    }
    // --- Slicing ---

    /// Get a slice of entries from `start` to `end` inclusive.
    ///
    /// Clamps to available range: `[max(start, snapshot_index+1), min(end, last_index)]`.
    pub fn slice(&self, start: LogIndex, end: LogIndex) -> &[Entry] {
        let actual_start = start.max(self.snapshot_index + 1);
        if actual_start > end || self.entries.is_empty() {
            return &[];
        }

        let Some(s) = self.to_vec_index(actual_start) else {
            return &[];
        };
        let e = self.to_vec_index(end.min(self.last_index())).map_or(self.entries.len(), |i| i + 1);

        if s >= self.entries.len() || s >= e {
            return &[];
        }
        &self.entries[s..e]
    }

    // --- Queries ---

    /// Check if log has no entries (may still have snapshot).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of entries currently in memory.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Find the latest configuration entry at or before the given index.
    ///
    /// Searches available entries plus the snapshot configuration.
    /// Returns None if no config entry exists before or at `index`.
    pub fn config_at(&self, index: LogIndex) -> Option<&Configuration> {
        if index <= self.snapshot_index {
            return self.snapshot_config.as_ref();
        }
        let end_vec_idx = self
            .to_vec_index(index)
            .map_or(self.entries.len(), |i| (i + 1).min(self.entries.len()));
        self.entries[..end_vec_idx]
            .iter()
            .rev()
            .find_map(|e| e.payload.as_config())
            .or(self.snapshot_config.as_ref())
    }

    /// Find the index of the latest uncommitted configuration entry.
    ///
    /// Returns Some(index) if there's a config entry after `commit_index`.
    pub fn pending_config_index(&self, commit_index: LogIndex) -> Option<LogIndex> {
        self.entries
            .iter()
            .rev()
            .find(|e| e.payload.is_config() && e.index > commit_index)
            .map(|e| e.index)
    }

    // --- Internal helpers ---

    /// Convert log index to vec index, accounting for snapshot.
    #[inline]
    fn to_vec_index(&self, index: LogIndex) -> Option<usize> {
        if index <= self.snapshot_index {
            None
        } else {
            usize::try_from(index - self.snapshot_index - 1).ok()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;

    fn entry(term: Term, index: LogIndex) -> Entry {
        Entry { term, index, payload: EntryPayload::Command(Command(vec![index as u8])) }
    }

    fn config_entry(term: Term, index: LogIndex, voters: Vec<NodeId>) -> Entry {
        Entry { term, index, payload: EntryPayload::Config(Configuration::simple(voters)) }
    }

    #[test]
    fn empty_log() {
        let log = Log::default();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), 0);
        assert_eq!(log.term_at(1), 0);
        assert!(log.get(0).is_none());
        assert!(log.get(1).is_none());
    }

    #[test]
    fn append_and_access() {
        let mut log = Log::default();
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(2, 3));

        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.term_at(1), 1);
        assert_eq!(log.term_at(2), 1);
        assert_eq!(log.term_at(3), 2);
        assert_eq!(log.get(2).unwrap().term, 1);
    }

    #[test]
    fn truncate() {
        let mut log = Log::default();
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(2, 3));

        log.truncate_after(1);
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.last_term(), 1);

        log.truncate_after(0);
        assert!(log.is_empty());
    }

    #[test]
    fn slice_entries() {
        let mut log = Log::default();
        for i in 1..=5 {
            log.append(entry(1, i));
        }

        let s = log.slice(2, 4);
        assert_eq!(s.len(), 3); // indices 2, 3, 4
        assert_eq!(s[0].index, 2);
        assert_eq!(s[2].index, 4);
    }

    // Log Matching Property (§3.5): same index+term implies same prefix
    #[test]
    fn log_matching_invariant() {
        let mut log1 = Log::default();
        let mut log2 = Log::default();

        // Same entries up to index 3
        for i in 1..=3 {
            log1.append(entry(1, i));
            log2.append(entry(1, i));
        }

        // If index 3 has same term in both logs, prefixes must match
        assert_eq!(log1.term_at(3), log2.term_at(3));
        for i in 1..=3 {
            assert_eq!(log1.term_at(i), log2.term_at(i));
        }
    }

    #[test]
    fn config_at_finds_latest() {
        let mut log = Log::default();

        // No config yet
        assert!(log.config_at(0).is_none());
        assert!(log.config_at(10).is_none());

        // Add some entries with a config in the middle
        log.append(entry(1, 1));
        log.append(config_entry(1, 2, vec![NodeId(0), NodeId(1)]));
        log.append(entry(1, 3));
        log.append(config_entry(1, 4, vec![NodeId(0), NodeId(1), NodeId(2)]));
        log.append(entry(1, 5));

        // At index 1, no config yet
        assert!(log.config_at(1).is_none());

        // At index 2, first config
        let c = log.config_at(2).unwrap();
        assert_eq!(*c, Configuration::simple(vec![NodeId(0), NodeId(1)]));

        // At index 3, still first config
        let c = log.config_at(3).unwrap();
        assert_eq!(*c, Configuration::simple(vec![NodeId(0), NodeId(1)]));

        // At index 4+, second config
        let c = log.config_at(4).unwrap();
        assert_eq!(*c, Configuration::simple(vec![NodeId(0), NodeId(1), NodeId(2)]));
    }

    #[test]
    fn pending_config_index() {
        let mut log = Log::default();

        // Empty log, commit_index 0
        assert!(log.pending_config_index(0).is_none());

        log.append(entry(1, 1));
        log.append(config_entry(1, 2, vec![NodeId(0)]));
        log.append(entry(1, 3));

        // Config at index 2 is pending if commit_index < 2
        assert_eq!(log.pending_config_index(0), Some(2));
        assert_eq!(log.pending_config_index(1), Some(2));

        // Config at index 2 is committed if commit_index >= 2
        assert!(log.pending_config_index(2).is_none());
        assert!(log.pending_config_index(3).is_none());
    }

    // --- Snapshot tests ---

    #[test]
    fn snapshot_metadata_initial() {
        let log = Log::default();
        assert_eq!(log.snapshot_index(), 0);
        assert_eq!(log.snapshot_term(), 0);
    }

    #[test]
    fn truncate_prefix_basic() {
        let mut log = Log::default();
        for i in 1..=5 {
            log.append(entry(1, i));
        }

        // Truncate prefix up to and including index 2
        log.truncate_prefix(2, 1);

        // Snapshot metadata updated
        assert_eq!(log.snapshot_index(), 2);
        assert_eq!(log.snapshot_term(), 1);

        // Entries 1, 2 are gone
        assert!(log.get(1).is_none());
        assert!(log.get(2).is_none());

        // Entries 3, 4, 5 remain
        assert!(log.get(3).is_some());
        assert_eq!(log.get(3).unwrap().index, 3);
        assert!(log.get(5).is_some());

        // last_index still correct
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.last_term(), 1);
    }

    #[test]
    fn truncate_prefix_all_entries() {
        let mut log = Log::default();
        for i in 1..=3 {
            log.append(entry(2, i));
        }

        // Truncate everything
        log.truncate_prefix(3, 2);

        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.snapshot_term(), 2);
        assert!(log.get(3).is_none());
        assert_eq!(log.last_index(), 3); // last_index == snapshot_index when empty
        assert_eq!(log.last_term(), 2); // last_term == snapshot_term when empty
    }

    #[test]
    fn term_at_with_snapshot() {
        let mut log = Log::default();
        for i in 1..=5 {
            log.append(entry(i, i)); // term == index for variety
        }

        log.truncate_prefix(2, 2);

        // Index 0 always returns 0
        assert_eq!(log.term_at(0), 0);
        // Discarded entries return 0 (caller should check snapshot_index)
        assert_eq!(log.term_at(1), 0);
        // Snapshot boundary keeps the term needed for AppendEntries matching
        assert_eq!(log.term_at(2), 2);
        // Remaining entries work
        assert_eq!(log.term_at(3), 3);
        assert_eq!(log.term_at(5), 5);
    }

    #[test]
    fn slice_with_snapshot() {
        let mut log = Log::default();
        for i in 1..=5 {
            log.append(entry(1, i));
        }

        log.truncate_prefix(2, 1);

        // Slice starting before snapshot returns from first available
        let s = log.slice(1, 5);
        assert_eq!(s.len(), 3); // indices 3, 4, 5
        assert_eq!(s[0].index, 3);

        // Slice fully within available range
        let s = log.slice(3, 4);
        assert_eq!(s.len(), 2);
        assert_eq!(s[0].index, 3);
        assert_eq!(s[1].index, 4);
    }

    #[test]
    fn append_after_snapshot() {
        let mut log = Log::default();
        for i in 1..=3 {
            log.append(entry(1, i));
        }

        log.truncate_prefix(3, 1);

        // Append new entry after snapshot
        log.append(entry(2, 4));
        assert_eq!(log.last_index(), 4);
        assert_eq!(log.last_term(), 2);
        assert!(log.get(4).is_some());
    }

    #[test]
    fn first_index_with_snapshot() {
        let mut log = Log::default();
        for i in 1..=5 {
            log.append(entry(1, i));
        }

        assert_eq!(log.first_index(), 1);

        log.truncate_prefix(3, 1);

        // First available index is now 4 (snapshot covers up to 3)
        assert_eq!(log.first_index(), 4);
    }

    #[test]
    fn first_index_empty_after_snapshot() {
        let mut log = Log::default();
        for i in 1..=3 {
            log.append(entry(1, i));
        }

        log.truncate_prefix(3, 1);

        // No entries remain, first_index returns snapshot_index + 1
        assert_eq!(log.first_index(), 4);
    }

    #[test]
    fn truncate_prefix_preserves_snapshot_configuration() {
        let mut log = Log::default();
        let config = Configuration::simple(vec![NodeId(0), NodeId(1)]);
        log.append(entry(1, 1));
        log.append(Entry { term: 1, index: 2, payload: EntryPayload::Config(config.clone()) });
        log.append(entry(1, 3));

        log.truncate_prefix(2, 1);

        assert_eq!(log.config_at(2), Some(&config));
        assert_eq!(log.config_at(3), Some(&config));
    }

    #[test]
    fn install_snapshot_retains_matching_suffix() {
        let mut log = Log::default();
        for i in 1..=5 {
            log.append(entry(1, i));
        }

        log.install_snapshot(3, 1, Configuration::simple(vec![NodeId(0)]));

        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.term_at(3), 1);
        assert!(log.get(4).is_some());
        assert!(log.get(5).is_some());
    }

    #[test]
    fn install_snapshot_discards_conflicting_suffix() {
        let mut log = Log::default();
        for i in 1..=5 {
            log.append(entry(1, i));
        }

        log.install_snapshot(3, 2, Configuration::simple(vec![NodeId(0)]));

        assert_eq!(log.snapshot_index(), 3);
        assert_eq!(log.snapshot_term(), 2);
        assert_eq!(log.last_index(), 3);
        assert!(log.is_empty());
    }
}

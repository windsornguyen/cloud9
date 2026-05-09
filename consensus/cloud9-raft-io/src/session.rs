//! Client session management for exactly-once semantics (§6.3).
//!
//! Per the Raft dissertation §6.3: "Raft implements linearizable semantics by
//! having leaders sequence all commands in the Raft log. However, if a leader
//! crashes after committing a command but before responding to the client, the
//! client might retry the command and have it executed twice."
//!
//! The solution is to track client sessions:
//! 1. Clients register and receive unique IDs
//! 2. Each request carries (`client_id`, `sequence_number`)
//! 3. The state machine tracks the last completed sequence per client
//! 4. Duplicate requests return cached responses
//!
//! # Important: Session State Must Be Replicated
//!
//! Session state must survive leader crashes, so it must be part of the
//! replicated state machine. This module provides types and a `SessionTracker`
//! that applications should embed in their replicated state.
//!
//! # Example
//!
//! ```ignore
//! // In your replicated state machine:
//! struct MyStateMachine {
//!     sessions: SessionTracker,
//!     // ... other state
//! }
//!
//! impl MyStateMachine {
//!     fn apply(&mut self, cmd: Command) -> Option<Response> {
//!         let req: SessionRequest = deserialize(&cmd.0);
//!
//!         // Check for duplicate
//!         if let Some(cached) = self.sessions.check_duplicate(req.client_id, req.sequence) {
//!             return Some(cached);
//!         }
//!
//!         // Execute the command
//!         let response = self.execute(req.data);
//!
//!         // Record completion
//!         self.sessions.record_completion(req.client_id, req.sequence, response.clone());
//!
//!         Some(response)
//!     }
//! }
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Unique identifier for a client session.
///
/// Assigned by the cluster when a client registers. Must be globally unique
/// across the cluster's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClientId(pub u64);

/// Monotonically increasing sequence number for client requests.
///
/// Each client maintains its own sequence counter, starting at 1.
/// The client increments this for each new request.
pub type SequenceNum = u64;

/// A client request with session tracking information.
///
/// Wrap your actual request payload with this to enable duplicate detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRequest<T> {
    /// The client making the request.
    pub client_id: ClientId,
    /// Sequence number for this request (monotonically increasing per client).
    pub sequence: SequenceNum,
    /// The actual request payload.
    pub payload: T,
}

impl<T> SessionRequest<T> {
    /// Create a new session request.
    pub fn new(client_id: ClientId, sequence: SequenceNum, payload: T) -> Self {
        Self { client_id, sequence, payload }
    }
}

/// State for a single client session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientSession<R> {
    /// Last completed sequence number.
    pub last_sequence: SequenceNum,
    /// Cached response for the last completed request.
    /// Used to respond to duplicate requests.
    pub last_response: Option<R>,
}

impl<R> Default for ClientSession<R> {
    fn default() -> Self {
        Self { last_sequence: 0, last_response: None }
    }
}

/// Result of checking for a duplicate request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuplicateCheck<R> {
    /// This is a new request that should be executed.
    New,
    /// This is a duplicate of an already-completed request.
    /// Returns the cached response.
    Duplicate(R),
    /// This request has a stale sequence number (older than last completed).
    /// The client is misbehaving or confused.
    Stale,
}

/// Tracks client sessions for duplicate detection (§6.3).
///
/// Embed this in your replicated state machine. It must be serialized and
/// replicated along with the rest of your state to survive leader crashes.
///
/// # Session Lifecycle
///
/// 1. Client calls `register_client()` → receives `ClientId`
/// 2. Client sends requests with `(client_id, sequence)` pairs
/// 3. State machine checks `check_duplicate()` before executing
/// 4. After execution, call `record_completion()` to cache response
/// 5. Optionally call `expire_session()` when clients disconnect
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionTracker<R> {
    /// Session state per client.
    sessions: BTreeMap<ClientId, ClientSession<R>>,
    /// Next client ID to assign.
    next_client_id: u64,
}

impl<R: Clone> SessionTracker<R> {
    /// Create a new session tracker.
    pub fn new() -> Self {
        Self { sessions: BTreeMap::new(), next_client_id: 1 }
    }

    /// Register a new client session.
    ///
    /// Returns a unique client ID. This should be called when processing
    /// a `RegisterClient` command from the replicated log.
    pub fn register_client(&mut self) -> ClientId {
        let id = ClientId(self.next_client_id);
        self.next_client_id += 1;
        self.sessions.insert(id, ClientSession::default());
        id
    }

    /// Check if a request is a duplicate.
    ///
    /// Call this before executing a command. Returns:
    /// - `New`: Execute the command
    /// - `Duplicate(response)`: Return the cached response, don't re-execute
    /// - `Stale`: Client sent an old sequence number, likely a bug
    pub fn check_duplicate(&self, client_id: ClientId, sequence: SequenceNum) -> DuplicateCheck<R> {
        let Some(session) = self.sessions.get(&client_id) else {
            // Unknown client - treat as new (client may have registered but
            // this replica hasn't seen it yet, or client is unregistered)
            return DuplicateCheck::New;
        };

        match sequence.cmp(&session.last_sequence) {
            std::cmp::Ordering::Greater => DuplicateCheck::New,
            std::cmp::Ordering::Equal => {
                // Exact duplicate - return cached response if available
                match &session.last_response {
                    Some(resp) => DuplicateCheck::Duplicate(resp.clone()),
                    None => DuplicateCheck::New, // No cached response, re-execute is safe
                }
            }
            std::cmp::Ordering::Less => DuplicateCheck::Stale,
        }
    }

    /// Record that a request completed successfully.
    ///
    /// Call this after executing a command. Caches the response for
    /// duplicate detection.
    pub fn record_completion(&mut self, client_id: ClientId, sequence: SequenceNum, response: R) {
        let session = self.sessions.entry(client_id).or_default();
        if sequence > session.last_sequence {
            session.last_sequence = sequence;
            session.last_response = Some(response);
        }
    }

    /// Expire a client session.
    ///
    /// Call this when a client disconnects or times out. Frees memory
    /// used by the session's cached response.
    pub fn expire_session(&mut self, client_id: ClientId) -> bool {
        self.sessions.remove(&client_id).is_some()
    }

    /// Check if a client is registered.
    pub fn is_registered(&self, client_id: ClientId) -> bool {
        self.sessions.contains_key(&client_id)
    }

    /// Number of active sessions.
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Get the last completed sequence for a client.
    pub fn last_sequence(&self, client_id: ClientId) -> Option<SequenceNum> {
        self.sessions.get(&client_id).map(|s| s.last_sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_client_returns_unique_ids() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();

        let id1 = tracker.register_client();
        let id2 = tracker.register_client();
        let id3 = tracker.register_client();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn new_request_returns_new() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();
        let client = tracker.register_client();

        assert!(matches!(tracker.check_duplicate(client, 1), DuplicateCheck::New));
    }

    #[test]
    fn duplicate_request_returns_cached_response() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();
        let client = tracker.register_client();

        // Complete first request
        tracker.record_completion(client, 1, "response1".to_string());

        // Same sequence should return cached response
        assert_eq!(
            tracker.check_duplicate(client, 1),
            DuplicateCheck::Duplicate("response1".to_string())
        );
    }

    #[test]
    fn stale_request_detected() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();
        let client = tracker.register_client();

        // Complete sequence 5
        tracker.record_completion(client, 5, "response5".to_string());

        // Sequence 3 is stale
        assert!(matches!(tracker.check_duplicate(client, 3), DuplicateCheck::Stale));
    }

    #[test]
    fn higher_sequence_is_new() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();
        let client = tracker.register_client();

        tracker.record_completion(client, 1, "response1".to_string());

        // Sequence 2 is new
        assert!(matches!(tracker.check_duplicate(client, 2), DuplicateCheck::New));
    }

    #[test]
    fn expire_session_removes_client() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();
        let client = tracker.register_client();

        assert!(tracker.is_registered(client));
        assert!(tracker.expire_session(client));
        assert!(!tracker.is_registered(client));
    }

    #[test]
    fn unknown_client_treated_as_new() {
        let tracker: SessionTracker<String> = SessionTracker::new();
        let unknown = ClientId(999);

        assert!(matches!(tracker.check_duplicate(unknown, 1), DuplicateCheck::New));
    }

    #[test]
    fn session_request_creation() {
        let req = SessionRequest::new(ClientId(1), 42, "payload");

        assert_eq!(req.client_id, ClientId(1));
        assert_eq!(req.sequence, 42);
        assert_eq!(req.payload, "payload");
    }

    #[test]
    fn record_completion_ignores_lower_sequence() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();
        let client = tracker.register_client();

        // Complete sequence 5
        tracker.record_completion(client, 5, "response5".to_string());

        // Try to record sequence 3 (should be ignored)
        tracker.record_completion(client, 3, "response3".to_string());

        // Last sequence should still be 5
        assert_eq!(tracker.last_sequence(client), Some(5));
        assert_eq!(
            tracker.check_duplicate(client, 5),
            DuplicateCheck::Duplicate("response5".to_string())
        );
    }

    #[test]
    fn tracker_serialization() {
        let mut tracker: SessionTracker<String> = SessionTracker::new();
        let client = tracker.register_client();
        tracker.record_completion(client, 1, "response".to_string());

        // Serialize and deserialize
        let json = serde_json::to_string(&tracker).unwrap();
        let restored: SessionTracker<String> = serde_json::from_str(&json).unwrap();

        // Should preserve state
        assert!(restored.is_registered(client));
        assert_eq!(
            restored.check_duplicate(client, 1),
            DuplicateCheck::Duplicate("response".to_string())
        );
    }
}

//! Transport trait for sending and receiving Raft messages.
//!
//! The transport layer handles network communication between Raft nodes.
//! It abstracts over the underlying protocol (TCP, QUIC, etc.).

use std::future::Future;

use cloud9_raft::NodeId;
use cloud9_raft::raft::Message;
use thiserror::Error;

/// Errors from transport operations.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("Connection failed to {0:?}: {1}")]
    ConnectionFailed(NodeId, String),

    #[error("Send failed to {0:?}: {1}")]
    SendFailed(NodeId, String),

    #[error("Receive failed: {0}")]
    ReceiveFailed(String),

    #[error("Node {0:?} not found in cluster")]
    NodeNotFound(NodeId),

    #[error("Transport closed")]
    Closed,
}

/// Transport trait for Raft message passing.
///
/// Implementations handle the network layer for Raft communication.
/// Messages may be delivered out of order, duplicated, or dropped —
/// Raft tolerates all of these.
///
/// # Connection Management
///
/// Implementations should handle connection pooling and reconnection
/// internally. The `send` method should not block on connection establishment.
///
/// # Thread Safety
///
/// Implementations must be safe to use from async contexts.
pub trait Transport: Send + Sync {
    /// Send a message to another node.
    ///
    /// This should be non-blocking (fire-and-forget). Raft handles retries
    /// via its heartbeat mechanism.
    fn send(&self, msg: Message) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Receive the next message.
    ///
    /// Blocks until a message is available or the transport is closed.
    fn recv(&self) -> impl Future<Output = Result<Message, TransportError>> + Send;

    /// Update the cluster membership.
    ///
    /// Called when configuration changes. The transport should establish
    /// connections to new nodes and may close connections to removed nodes.
    fn update_peers(
        &self,
        peers: &[NodeId],
    ) -> impl Future<Output = Result<(), TransportError>> + Send;
}

#[cfg(test)]
mod tests {
    // Tests will use in-memory channel implementation
}

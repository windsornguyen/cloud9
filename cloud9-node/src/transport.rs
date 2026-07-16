//! Raft peer HTTP transport.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router as AxumRouter;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use cloud9_raft::NodeId;
use cloud9_raft::raft::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::{Duration, timeout};
use tracing::warn;

use crate::RaftKey;
use crate::runtime::RaftRuntime;

const RAFT_RPC_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RESPONSE_BYTES: u64 = 8 * 1024;
const MAX_RAFT_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
const MAX_IN_FLIGHT_PER_PEER: usize = 16;
const SIGNATURE_HEADER: &str = "x-cloud9-raft-signature";

struct Peer {
    addr: SocketAddr,
    permits: Arc<Semaphore>,
}

pub(crate) struct PeerTransport {
    peers: BTreeMap<NodeId, Peer>,
    key: RaftKey,
}

impl PeerTransport {
    pub(crate) fn new(node_id: NodeId, key: RaftKey, peers: &BTreeMap<NodeId, SocketAddr>) -> Self {
        let peers = peers
            .iter()
            .filter(|(peer, _)| **peer != node_id)
            .map(|(peer, addr)| {
                (
                    *peer,
                    Peer { addr: *addr, permits: Arc::new(Semaphore::new(MAX_IN_FLIGHT_PER_PEER)) },
                )
            })
            .collect();
        Self { peers, key }
    }

    pub(crate) fn send(&self, message: Message) -> Result<(), NodeId> {
        let peer = self.peers.get(&message.to).ok_or(message.to)?;
        let Ok(permit) = peer.permits.clone().try_acquire_owned() else {
            warn!(to = message.to.0, "dropping Raft message at peer concurrency limit");
            return Ok(());
        };
        let addr = peer.addr;
        let key = self.key.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = post_raft_message(addr, &key, &message).await {
                warn!(%error, to = message.to.0, "failed to send Raft message");
            }
        });
        Ok(())
    }
}

pub(crate) fn raft_app(runtime: Arc<RaftRuntime>) -> AxumRouter {
    AxumRouter::new()
        .route("/raft/message", post(receive_raft))
        .layer(DefaultBodyLimit::max(MAX_RAFT_MESSAGE_BYTES))
        .with_state(runtime)
}

async fn receive_raft(
    State(runtime): State<Arc<RaftRuntime>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, (StatusCode, String)> {
    let signature = headers
        .get(SIGNATURE_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or((StatusCode::UNAUTHORIZED, "missing Raft signature".to_owned()))?;
    if !runtime.verify_signature(&body, signature) {
        return Err((StatusCode::UNAUTHORIZED, "invalid Raft signature".to_owned()));
    }
    let message = serde_json::from_slice(&body)
        .map_err(|error| (StatusCode::BAD_REQUEST, format!("invalid Raft message: {error}")))?;
    runtime
        .validate_message(&message)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    runtime
        .step(message)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn post_raft_message(
    addr: SocketAddr,
    key: &RaftKey,
    message: &Message,
) -> Result<()> {
    timeout(RAFT_RPC_TIMEOUT, post_raft_message_inner(addr, key, message))
        .await
        .with_context(|| format!("Raft RPC to {addr} timed out"))?
}

async fn post_raft_message_inner(addr: SocketAddr, key: &RaftKey, message: &Message) -> Result<()> {
    let body = serde_json::to_vec(message).context("encoding Raft message")?;
    if body.len() > MAX_RAFT_MESSAGE_BYTES {
        anyhow::bail!("encoded Raft message exceeds {MAX_RAFT_MESSAGE_BYTES}-byte limit");
    }
    let signature = key.signature(&body).context("signing Raft message")?;
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to Raft peer {addr}"))?;
    let request = format!(
        "POST /raft/message HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Type: application/json\r\n\
         {SIGNATURE_HEADER}: {signature}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.context("writing Raft message headers")?;
    stream.write_all(&body).await.context("writing Raft message body")?;

    let mut response = Vec::new();
    stream
        .take(MAX_RESPONSE_BYTES)
        .read_to_end(&mut response)
        .await
        .context("reading Raft message response")?;
    if response.starts_with(b"HTTP/1.1 204") {
        return Ok(());
    }

    let response = String::from_utf8_lossy(&response);
    anyhow::bail!("Raft peer {addr} rejected message: {response}");
}

#[cfg(test)]
mod tests {
    use cloud9_raft::raft::{AppendRequest, Entry, EntryPayload, Payload};
    use cloud9_raft::{Command, NodeId};

    use super::*;
    use crate::command::{
        KvCommand, MAX_ETAG_BYTES, MAX_KEY_BYTES, MAX_NAMESPACE_BYTES, MAX_VALUE_BYTES,
    };

    #[test]
    fn invariant_largest_command_fits_one_raft_message() {
        let command = KvCommand::Put {
            client_id: u64::MAX,
            sequence: u64::MAX,
            namespace: "\u{1}".repeat(MAX_NAMESPACE_BYTES),
            key: "\u{1}".repeat(MAX_KEY_BYTES),
            body: vec![u8::MAX; MAX_VALUE_BYTES],
            if_match: "\u{1}".repeat(MAX_ETAG_BYTES),
            if_none_match: false,
        };
        let command = Command(serde_json::to_vec(&command).unwrap());
        let message = Message {
            from: NodeId(u64::MAX),
            to: NodeId(u64::MAX),
            term: u64::MAX,
            payload: Payload::AppendRequest(AppendRequest {
                prev_log_index: u64::MAX,
                prev_log_term: u64::MAX,
                entries: vec![Entry {
                    term: u64::MAX,
                    index: u64::MAX,
                    payload: EntryPayload::Command(command),
                }],
                leader_commit: u64::MAX,
            }),
        };

        let encoded = serde_json::to_vec(&message).unwrap();

        assert!(encoded.len() <= MAX_RAFT_MESSAGE_BYTES, "encoded {} bytes", encoded.len());
    }
}

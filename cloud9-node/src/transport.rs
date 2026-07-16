//! Raft peer HTTP transport.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::Json;
use axum::Router as AxumRouter;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use cloud9_raft::raft::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

use std::sync::Arc;

use crate::runtime::RaftRuntime;

const RAFT_RPC_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_RESPONSE_BYTES: u64 = 8 * 1024;

pub(crate) fn raft_app(runtime: Arc<RaftRuntime>) -> AxumRouter {
    AxumRouter::new().route("/raft/message", post(receive_raft)).with_state(runtime)
}

async fn receive_raft(
    State(runtime): State<Arc<RaftRuntime>>,
    Json(message): Json<Message>,
) -> Result<StatusCode, (StatusCode, String)> {
    runtime
        .validate_message(&message)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    runtime
        .step(message)
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn post_raft_message(addr: SocketAddr, message: &Message) -> Result<()> {
    timeout(RAFT_RPC_TIMEOUT, post_raft_message_inner(addr, message))
        .await
        .with_context(|| format!("Raft RPC to {addr} timed out"))?
}

async fn post_raft_message_inner(addr: SocketAddr, message: &Message) -> Result<()> {
    let body = serde_json::to_vec(message).context("encoding Raft message")?;
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to Raft peer {addr}"))?;
    let request = format!(
        "POST /raft/message HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Type: application/json\r\n\
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

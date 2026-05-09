//! Top-level orchestration for Cloud9 nodes.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Json;
use axum::Router as AxumRouter;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use cloud9_proto::generated::cloud9::kv::v1::{
    DeleteResponse, GetResponse, HeadResponse, KvService, KvServiceExt, OwnedDeleteRequestView,
    OwnedGetRequestView, OwnedHeadRequestView, OwnedPutRequestView,
    OwnedRegisterSessionRequestView, OwnedStatusRequestView, PutResponse, RegisterSessionResponse,
    StatusResponse,
};
use cloud9_raft::raft::{Effects, Message};
use cloud9_raft::{Command, ConsensusConfig, LogIndex, NodeId, ProposeError, RaftNode};
use cloud9_storage::StorageOptions;
use connectrpc::{ConnectError, RequestContext, Response, Router as ConnectRouter, ServiceResult};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::{Duration, sleep};
use tracing::{info, instrument, warn};

/// Runtime configuration derived from CLI flags and config files.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub client_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    pub peers: BTreeMap<NodeId, SocketAddr>,
    pub storage: StorageOptions,
    pub consensus: ConsensusConfig,
}

impl Default for NodeConfig {
    fn default() -> Self {
        let node_id = NodeId(0);
        let raft_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 19_091));
        Self {
            node_id,
            client_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 19_090)),
            raft_addr,
            peers: BTreeMap::from([(node_id, raft_addr)]),
            storage: StorageOptions::default(),
            consensus: raft_config(node_id),
        }
    }
}

pub fn raft_config(node_id: NodeId) -> ConsensusConfig {
    ConsensusConfig::new(node_id).with_parallel_disk_write(false)
}

#[derive(Clone)]
struct KvApi {
    config: NodeConfig,
    state: Arc<RwLock<KvState>>,
    runtime: Arc<RaftRuntime>,
}

struct RaftRuntime {
    config: NodeConfig,
    node: Mutex<RaftNode>,
    state: Arc<RwLock<KvState>>,
    waiters: Mutex<HashMap<LogIndex, oneshot::Sender<Result<KvApplyResult, ConnectError>>>>,
}

impl RaftRuntime {
    fn new(config: NodeConfig, state: Arc<RwLock<KvState>>) -> Self {
        let voters = config.peers.keys().copied().collect::<Vec<_>>();
        Self {
            node: Mutex::new(RaftNode::new(config.consensus.clone(), &voters)),
            config,
            state,
            waiters: Mutex::new(HashMap::new()),
        }
    }

    fn spawn(self: Arc<Self>) {
        tokio::spawn(async move {
            self.tick_loop().await;
        });
    }

    async fn tick_loop(&self) {
        loop {
            sleep(Duration::from_millis(1)).await;
            let mut node = self.node.lock().await;
            let effects = node.tick();
            self.handle_effects(&mut node, effects).await;
        }
    }

    async fn step(&self, message: Message) {
        let mut node = self.node.lock().await;
        let effects = node.step(message);
        self.handle_effects(&mut node, effects).await;
    }

    async fn propose(&self, command: KvCommand) -> Result<KvApplyResult, ConnectError> {
        let bytes = serde_json::to_vec(&command)
            .map_err(|_| ConnectError::internal("failed to encode Raft command"))?;
        let receiver = {
            let mut node = self.node.lock().await;
            let (index, effects) =
                node.propose(Command(bytes)).map_err(|error| propose_error(&error))?;
            let (sender, receiver) = oneshot::channel();
            self.waiters.lock().await.insert(index, sender);
            self.handle_effects(&mut node, effects).await;
            receiver
        };
        receiver.await.map_err(|_| ConnectError::aborted("Raft proposal was dropped"))?
    }

    async fn read_barrier(&self) -> Result<(), ConnectError> {
        match self.propose(KvCommand::ReadBarrier).await? {
            KvApplyResult::ReadBarrier => Ok(()),
            KvApplyResult::RegisterSession(_)
            | KvApplyResult::Put(_)
            | KvApplyResult::Delete(_) => Err(ConnectError::internal("Raft read barrier mismatch")),
        }
    }

    async fn mode(&self) -> String {
        let node = self.node.lock().await;
        if node.is_leader() {
            "leader"
        } else if node.is_candidate() {
            "candidate"
        } else if node.is_precandidate() {
            "precandidate"
        } else {
            "follower"
        }
        .to_owned()
    }

    async fn handle_effects(&self, node: &mut RaftNode, effects: Effects) {
        if !effects.send_snapshots.is_empty() {
            warn!(
                snapshot_count = effects.send_snapshots.len(),
                "Raft snapshot transport is not implemented"
            );
        }

        for message in effects.messages {
            self.send_message(message);
        }

        self.apply_committed(node).await;
    }

    async fn apply_committed(&self, node: &mut RaftNode) {
        let entries = node.committed().collect::<Vec<_>>();
        let mut applied_to = None;
        for entry in entries {
            let result = self.apply_command(&entry.command).await;
            self.complete_waiter(entry.index, result).await;
            applied_to = Some(entry.index);
        }
        if let Some(index) = applied_to {
            node.advance(index);
        }
    }

    async fn apply_command(&self, command: &Command) -> Result<KvApplyResult, ConnectError> {
        let command = serde_json::from_slice(&command.0)
            .map_err(|_| ConnectError::internal("invalid Raft command payload"))?;
        self.state.write().await.apply(command)
    }

    async fn complete_waiter(&self, index: LogIndex, result: Result<KvApplyResult, ConnectError>) {
        if let Some(sender) = self.waiters.lock().await.remove(&index) {
            let _ = sender.send(result);
        }
    }

    fn send_message(&self, message: Message) {
        let Some(addr) = self.config.peers.get(&message.to).copied() else {
            warn!(to = message.to.0, "Raft message target is not in cluster config");
            return;
        };
        tokio::spawn(async move {
            if let Err(error) = post_raft_message(addr, &message).await {
                warn!(%error, to = message.to.0, "failed to send Raft message");
            }
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum KvCommand {
    RegisterSession,
    ReadBarrier,
    Put {
        client_id: u64,
        sequence: u64,
        namespace: String,
        key: String,
        body: Vec<u8>,
        if_match: String,
        if_none_match: bool,
    },
    Delete {
        client_id: u64,
        sequence: u64,
        namespace: String,
        key: String,
        if_match: String,
    },
}

enum KvApplyResult {
    RegisterSession(RegisterSessionResponse),
    Put(PutResponse),
    Delete(DeleteResponse),
    ReadBarrier,
}

#[derive(Default)]
struct KvState {
    next_client_id: u64,
    next_generation: u64,
    entries: HashMap<KvName, KvRecord>,
    sessions: HashMap<u64, SessionState>,
}

impl KvState {
    fn new() -> Self {
        Self { next_client_id: 1, next_generation: 1, ..Self::default() }
    }

    fn apply(&mut self, command: KvCommand) -> Result<KvApplyResult, ConnectError> {
        match command {
            KvCommand::RegisterSession => {
                let client_id = self.next_client_id()?;
                Ok(KvApplyResult::RegisterSession(RegisterSessionResponse {
                    client_id,
                    ..Default::default()
                }))
            }
            KvCommand::ReadBarrier => Ok(KvApplyResult::ReadBarrier),
            KvCommand::Put {
                client_id,
                sequence,
                namespace,
                key,
                body,
                if_match,
                if_none_match,
            } => self.apply_put(
                client_id,
                sequence,
                &namespace,
                &key,
                body,
                &if_match,
                if_none_match,
            ),
            KvCommand::Delete { client_id, sequence, namespace, key, if_match } => {
                self.apply_delete(client_id, sequence, &namespace, &key, &if_match)
            }
        }
    }

    fn apply_put(
        &mut self,
        client_id: u64,
        sequence: u64,
        namespace: &str,
        key: &str,
        body: Vec<u8>,
        if_match: &str,
        if_none_match: bool,
    ) -> Result<KvApplyResult, ConnectError> {
        validate_mutation_request(client_id, sequence)?;
        validate_put_preconditions(if_match, if_none_match)?;

        let name = KvName::new(namespace, key)?;
        if let Some(response) = cached_put(self, client_id, sequence)? {
            return Ok(KvApplyResult::Put(response));
        }

        let current = self.entries.get(&name);
        check_put_preconditions(current, if_match, if_none_match)?;

        let generation = self.next_generation()?;
        let etag = etag_for(generation);
        let response = PutResponse {
            namespace: name.namespace.clone(),
            key: name.key.clone(),
            etag: etag.clone(),
            generation,
            size: body_len(&body)?,
            ..Default::default()
        };
        self.entries.insert(name, KvRecord { body, etag, generation });
        self.session_mut(client_id)?.record(sequence, MutationResult::Put(response.clone()));
        Ok(KvApplyResult::Put(response))
    }

    fn apply_delete(
        &mut self,
        client_id: u64,
        sequence: u64,
        namespace: &str,
        key: &str,
        if_match: &str,
    ) -> Result<KvApplyResult, ConnectError> {
        validate_mutation_request(client_id, sequence)?;

        let name = KvName::new(namespace, key)?;
        if let Some(response) = cached_delete(self, client_id, sequence)? {
            return Ok(KvApplyResult::Delete(response));
        }

        let removed = if let Some(record) = self.entries.get(&name) {
            if !if_match.is_empty() && if_match != record.etag {
                return Err(ConnectError::failed_precondition("ETag precondition failed"));
            }
            self.entries.remove(&name)
        } else {
            if !if_match.is_empty() {
                return Err(ConnectError::failed_precondition("ETag precondition failed"));
            }
            None
        };

        let response = if let Some(record) = removed {
            DeleteResponse {
                namespace: name.namespace,
                key: name.key,
                etag: record.etag,
                generation: record.generation,
                deleted: true,
                ..Default::default()
            }
        } else {
            DeleteResponse {
                namespace: name.namespace,
                key: name.key,
                etag: String::new(),
                generation: 0,
                deleted: false,
                ..Default::default()
            }
        };
        self.session_mut(client_id)?.record(sequence, MutationResult::Delete(response.clone()));
        Ok(KvApplyResult::Delete(response))
    }

    fn next_client_id(&mut self) -> Result<u64, ConnectError> {
        let client_id = self.next_client_id;
        self.next_client_id = self
            .next_client_id
            .checked_add(1)
            .ok_or_else(|| ConnectError::resource_exhausted("client id space exhausted"))?;
        self.sessions.insert(client_id, SessionState::default());
        Ok(client_id)
    }

    fn next_generation(&mut self) -> Result<u64, ConnectError> {
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .ok_or_else(|| ConnectError::resource_exhausted("kv generation space exhausted"))?;
        Ok(generation)
    }

    fn session(&self, client_id: u64) -> Result<&SessionState, ConnectError> {
        self.sessions
            .get(&client_id)
            .ok_or_else(|| ConnectError::invalid_argument("unknown client session"))
    }

    fn session_mut(&mut self, client_id: u64) -> Result<&mut SessionState, ConnectError> {
        self.sessions
            .get_mut(&client_id)
            .ok_or_else(|| ConnectError::invalid_argument("unknown client session"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct KvName {
    namespace: String,
    key: String,
}

impl KvName {
    fn new(namespace: &str, key: &str) -> Result<Self, ConnectError> {
        if namespace.is_empty() {
            return Err(ConnectError::invalid_argument("namespace must not be empty"));
        }
        if key.is_empty() {
            return Err(ConnectError::invalid_argument("key must not be empty"));
        }
        Ok(Self { namespace: namespace.to_owned(), key: key.to_owned() })
    }
}

#[derive(Clone)]
struct KvRecord {
    body: Vec<u8>,
    etag: String,
    generation: u64,
}

#[derive(Clone, Default)]
struct SessionState {
    last_sequence: u64,
    last_result: Option<MutationResult>,
}

#[derive(Clone)]
enum MutationResult {
    Put(PutResponse),
    Delete(DeleteResponse),
}

/// Launch the node's public KV API and Raft peer API.
#[instrument(skip_all)]
pub async fn launch(config: NodeConfig) -> Result<()> {
    info!(?config.storage, "initializing storage");
    info!(?config.consensus, "consensus subsystem configured");

    let state = Arc::new(RwLock::new(KvState::new()));
    let runtime = Arc::new(RaftRuntime::new(config.clone(), state.clone()));
    let api = Arc::new(KvApi { config: config.clone(), state, runtime: runtime.clone() });
    let client_app = kv_app(api);
    let raft_app = raft_app(runtime.clone());
    let client_listener = TcpListener::bind(config.client_addr)
        .await
        .with_context(|| format!("binding Cloud9 KV API to {}", config.client_addr))?;
    let raft_listener = TcpListener::bind(config.raft_addr)
        .await
        .with_context(|| format!("binding Cloud9 Raft API to {}", config.raft_addr))?;

    info!(
        node_id = config.node_id.0,
        client_addr = %config.client_addr,
        raft_addr = %config.raft_addr,
        peer_count = config.peers.len(),
        "serving Cloud9 KV API"
    );

    runtime.spawn();
    tokio::try_join!(
        axum::serve(client_listener, client_app),
        axum::serve(raft_listener, raft_app),
    )
    .context("serving Cloud9 node")?;
    Ok(())
}

fn kv_app(api: Arc<KvApi>) -> AxumRouter {
    let connect = api.register(ConnectRouter::new());
    AxumRouter::new()
        .route("/healthz", get(|| async { "ok" }))
        .fallback_service(connect.into_axum_service())
}

fn raft_app(runtime: Arc<RaftRuntime>) -> AxumRouter {
    AxumRouter::new().route("/raft/message", post(receive_raft)).with_state(runtime)
}

async fn receive_raft(
    State(runtime): State<Arc<RaftRuntime>>,
    Json(message): Json<Message>,
) -> StatusCode {
    runtime.step(message).await;
    StatusCode::NO_CONTENT
}

#[allow(refining_impl_trait)]
impl KvService for KvApi {
    async fn register_session(
        &self,
        _ctx: RequestContext,
        _request: OwnedRegisterSessionRequestView,
    ) -> ServiceResult<RegisterSessionResponse> {
        match self.runtime.propose(KvCommand::RegisterSession).await? {
            KvApplyResult::RegisterSession(response) => Response::ok(response),
            KvApplyResult::Put(_) | KvApplyResult::Delete(_) | KvApplyResult::ReadBarrier => {
                Err(ConnectError::internal("Raft session command mismatch"))
            }
        }
    }

    async fn head(
        &self,
        _ctx: RequestContext,
        request: OwnedHeadRequestView,
    ) -> ServiceResult<HeadResponse> {
        let name = KvName::new(request.namespace, request.key)?;
        self.runtime.read_barrier().await?;
        let state = self.state.read().await;
        let record = state.entries.get(&name).ok_or_else(key_not_found)?;
        Response::ok(HeadResponse {
            namespace: name.namespace,
            key: name.key,
            etag: record.etag.clone(),
            generation: record.generation,
            size: body_len(&record.body)?,
            ..Default::default()
        })
    }

    async fn get(
        &self,
        _ctx: RequestContext,
        request: OwnedGetRequestView,
    ) -> ServiceResult<GetResponse> {
        let name = KvName::new(request.namespace, request.key)?;
        self.runtime.read_barrier().await?;
        let state = self.state.read().await;
        let record = state.entries.get(&name).ok_or_else(key_not_found)?;
        Response::ok(GetResponse {
            namespace: name.namespace,
            key: name.key,
            etag: record.etag.clone(),
            generation: record.generation,
            size: body_len(&record.body)?,
            body: record.body.clone(),
            ..Default::default()
        })
    }

    async fn put(
        &self,
        _ctx: RequestContext,
        request: OwnedPutRequestView,
    ) -> ServiceResult<PutResponse> {
        validate_mutation_request(request.client_id, request.sequence)?;
        validate_put_preconditions(request.if_match, request.if_none_match)?;
        KvName::new(request.namespace, request.key)?;

        let command = KvCommand::Put {
            client_id: request.client_id,
            sequence: request.sequence,
            namespace: request.namespace.to_owned(),
            key: request.key.to_owned(),
            body: request.body.to_vec(),
            if_match: request.if_match.to_owned(),
            if_none_match: request.if_none_match,
        };
        match self.runtime.propose(command).await? {
            KvApplyResult::Put(response) => Response::ok(response),
            KvApplyResult::RegisterSession(_)
            | KvApplyResult::Delete(_)
            | KvApplyResult::ReadBarrier => {
                Err(ConnectError::internal("Raft put command mismatch"))
            }
        }
    }

    async fn delete(
        &self,
        _ctx: RequestContext,
        request: OwnedDeleteRequestView,
    ) -> ServiceResult<DeleteResponse> {
        validate_mutation_request(request.client_id, request.sequence)?;
        KvName::new(request.namespace, request.key)?;

        let command = KvCommand::Delete {
            client_id: request.client_id,
            sequence: request.sequence,
            namespace: request.namespace.to_owned(),
            key: request.key.to_owned(),
            if_match: request.if_match.to_owned(),
        };
        match self.runtime.propose(command).await? {
            KvApplyResult::Delete(response) => Response::ok(response),
            KvApplyResult::RegisterSession(_)
            | KvApplyResult::Put(_)
            | KvApplyResult::ReadBarrier => {
                Err(ConnectError::internal("Raft delete command mismatch"))
            }
        }
    }

    async fn status(
        &self,
        _ctx: RequestContext,
        _request: OwnedStatusRequestView,
    ) -> ServiceResult<StatusResponse> {
        let mode = self.runtime.mode().await;
        let state = self.state.read().await;
        Response::ok(StatusResponse {
            node_id: self.config.node_id.0,
            mode,
            key_count: usize_to_u64(state.entries.len()),
            ..Default::default()
        })
    }
}

impl SessionState {
    fn record(&mut self, sequence: u64, result: MutationResult) {
        self.last_sequence = sequence;
        self.last_result = Some(result);
    }
}

fn validate_mutation_request(client_id: u64, sequence: u64) -> Result<(), ConnectError> {
    if client_id == 0 {
        return Err(ConnectError::invalid_argument("client_id must be registered"));
    }
    if sequence == 0 {
        return Err(ConnectError::invalid_argument("sequence must be positive"));
    }
    Ok(())
}

fn validate_put_preconditions(if_match: &str, if_none_match: bool) -> Result<(), ConnectError> {
    if !if_match.is_empty() && if_none_match {
        return Err(ConnectError::invalid_argument(
            "if_match and if_none_match are mutually exclusive",
        ));
    }
    Ok(())
}

fn check_put_preconditions(
    current: Option<&KvRecord>,
    if_match: &str,
    if_none_match: bool,
) -> Result<(), ConnectError> {
    if if_none_match && current.is_some() {
        return Err(ConnectError::failed_precondition("key already exists"));
    }

    if !if_match.is_empty() {
        match current {
            Some(record) if record.etag == if_match => {}
            Some(_) | None => {
                return Err(ConnectError::failed_precondition("ETag precondition failed"));
            }
        }
    }

    Ok(())
}

fn cached_put(
    state: &KvState,
    client_id: u64,
    sequence: u64,
) -> Result<Option<PutResponse>, ConnectError> {
    match cached_mutation(state.session(client_id)?, sequence)? {
        Some(MutationResult::Put(response)) => Ok(Some(response)),
        Some(MutationResult::Delete(_)) => {
            Err(ConnectError::aborted("sequence reused for different operation"))
        }
        None => Ok(None),
    }
}

fn cached_delete(
    state: &KvState,
    client_id: u64,
    sequence: u64,
) -> Result<Option<DeleteResponse>, ConnectError> {
    match cached_mutation(state.session(client_id)?, sequence)? {
        Some(MutationResult::Delete(response)) => Ok(Some(response)),
        Some(MutationResult::Put(_)) => {
            Err(ConnectError::aborted("sequence reused for different operation"))
        }
        None => Ok(None),
    }
}

fn cached_mutation(
    session: &SessionState,
    sequence: u64,
) -> Result<Option<MutationResult>, ConnectError> {
    match sequence.cmp(&session.last_sequence) {
        Ordering::Less => Err(ConnectError::aborted("stale client sequence")),
        Ordering::Equal => Ok(session.last_result.clone()),
        Ordering::Greater => Ok(None),
    }
}

fn etag_for(generation: u64) -> String {
    format!("\"c9-{generation}\"")
}

fn body_len(body: &[u8]) -> Result<u64, ConnectError> {
    u64::try_from(body.len()).map_err(|_| ConnectError::resource_exhausted("value too large"))
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn key_not_found() -> ConnectError {
    ConnectError::not_found("key not found")
}

fn propose_error(error: &ProposeError) -> ConnectError {
    match error {
        ProposeError::NotLeader { leader_hint: Some(leader) } => {
            ConnectError::failed_precondition(format!("not leader; leader is {}", leader.0))
        }
        ProposeError::NotLeader { leader_hint: None } => {
            ConnectError::failed_precondition("not leader; leader unknown")
        }
        ProposeError::Throttled => ConnectError::resource_exhausted("too many Raft proposals"),
    }
}

async fn post_raft_message(addr: SocketAddr, message: &Message) -> Result<()> {
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
    stream.read_to_end(&mut response).await.context("reading Raft message response")?;
    if response.starts_with(b"HTTP/1.1 204") || response.starts_with(b"HTTP/1.1 200") {
        return Ok(());
    }

    let response = String::from_utf8_lossy(&response);
    anyhow::bail!("Raft peer {addr} rejected message: {response}");
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::time::Duration;

    use anyhow::bail;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kv_api_enforces_etag_preconditions() -> Result<()> {
        let config = NodeConfig::default();
        let state = Arc::new(RwLock::new(KvState::new()));
        let runtime = Arc::new(RaftRuntime::new(config.clone(), state.clone()));
        let api = Arc::new(KvApi { config: config.clone(), state, runtime: runtime.clone() });
        let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(axum::serve(listener, kv_app(api)).into_future());
        runtime.spawn();
        wait_for_leader(addr).await?;

        let (status, body) = post_json(addr, "RegisterSession", "{}")?;
        assert_eq!(200, status);
        assert!(body.contains("\"clientId\":\"1\""));

        let (status, _) = post_json(
            addr,
            "Put",
            r#"{"clientId":"1","sequence":"1","namespace":"jepsen","key":"register","body":"MQ==","ifNoneMatch":true}"#,
        )?;
        assert_eq!(200, status);

        let (status, _) = post_json(
            addr,
            "Put",
            r#"{"clientId":"1","sequence":"2","namespace":"jepsen","key":"register","body":"Mg==","ifNoneMatch":true}"#,
        )?;
        assert_eq!(400, status);

        let (status, _) = post_json(
            addr,
            "Put",
            r#"{"clientId":"1","sequence":"3","namespace":"jepsen","key":"register","body":"Mg==","ifMatch":"\"c9-1\""}"#,
        )?;
        assert_eq!(200, status);

        let (status, body) = post_json(addr, "Get", r#"{"namespace":"jepsen","key":"register"}"#)?;
        assert_eq!(200, status);
        assert!(body.contains("\"body\":\"Mg==\""));

        let (status, _) = post_json(
            addr,
            "Put",
            r#"{"clientId":"1","sequence":"4","namespace":"jepsen","key":"register","body":"Mw==","ifMatch":"\"c9-1\""}"#,
        )?;
        assert_eq!(400, status);

        server.abort();
        Ok(())
    }

    async fn wait_for_leader(addr: SocketAddr) -> Result<()> {
        for _ in 0..100 {
            let (status, body) = post_json(addr, "Status", "{}")?;
            if status == 200 && body.contains("\"mode\":\"leader\"") {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        bail!("single-node Raft runtime did not elect a leader");
    }

    fn post_json(addr: SocketAddr, method: &str, body: &str) -> Result<(u16, String)> {
        let mut stream = std::net::TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let request = format!(
            "POST /cloud9.kv.v1.KvService/{method} HTTP/1.1\r\n\
             Host: {addr}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {body}",
            body.len()
        );
        stream.write_all(request.as_bytes())?;

        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        let Some((head, body)) = response.split_once("\r\n\r\n") else {
            bail!("HTTP response missing header separator");
        };
        let Some(status) = head.lines().next().and_then(|line| line.split_whitespace().nth(1))
        else {
            bail!("HTTP response missing status");
        };
        let body = if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
            decode_chunked(body)?
        } else {
            body.to_owned()
        };

        Ok((status.parse()?, body))
    }

    fn decode_chunked(mut body: &str) -> Result<String> {
        let mut decoded = String::new();
        loop {
            let Some((len, rest)) = body.split_once("\r\n") else {
                bail!("chunk missing length");
            };
            let len = usize::from_str_radix(len.trim(), 16)?;
            if len == 0 {
                return Ok(decoded);
            }
            if rest.len() < len + 2 {
                bail!("chunk shorter than declared length");
            }
            decoded.push_str(&rest[..len]);
            body = &rest[len + 2..];
        }
    }
}

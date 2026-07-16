//! Async driver for the pure Raft state machine.
//!
//! Every step is serialized with its WAL. Persistent state is synced before
//! network effects are released, then committed commands are applied in order.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard};

use cloud9_raft::raft::{Effects, Message};
use cloud9_raft::{Command, LogIndex, NodeId, ProposeError, RaftNode};
use connectrpc::ConnectError;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::{Duration, sleep, timeout};

use crate::command::{KvApplyResult, KvCommand, KvState};
use crate::config::NodeConfig;
use crate::store::{RaftStore, StoreError};
use crate::transport::PeerTransport;

const TICK_INTERVAL: Duration = Duration::from_millis(1);
const PROPOSAL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub(crate) enum RuntimeError {
    #[error("node id {node_id} does not match consensus id {consensus_id}")]
    IdentityMismatch { node_id: NodeId, consensus_id: NodeId },
    #[error("cluster peers do not map node {node_id} to its Raft address {raft_addr}")]
    InvalidSelfPeer { node_id: NodeId, raft_addr: std::net::SocketAddr },
    #[error("cluster peers contain duplicate Raft address {address}")]
    DuplicatePeerAddress { address: std::net::SocketAddr },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("Raft snapshot transport is not implemented")]
    SnapshotTransportUnsupported,
    #[error("Raft emitted a message for unknown peer {peer}")]
    UnknownPeer { peer: NodeId },
    #[error("Raft runtime previously failed")]
    Failed,
}

#[derive(Debug, Error)]
pub(crate) enum MessageError {
    #[error("Raft message addressed to {actual}, expected {expected}")]
    WrongRecipient { expected: NodeId, actual: NodeId },
    #[error("Raft message sender {sender} is not in the cluster")]
    UnknownSender { sender: NodeId },
    #[error("Raft message claims this node as its sender")]
    SelfMessage,
}

struct RaftMachine {
    node: RaftNode,
    store: RaftStore,
}

struct PendingProposal {
    id: u64,
    command: Vec<u8>,
    sender: oneshot::Sender<Result<KvApplyResult, ConnectError>>,
}

impl PendingProposal {
    fn complete(self, command: &Command, result: Result<KvApplyResult, ConnectError>) {
        let result = if self.command == command.0 {
            result
        } else {
            Err(ConnectError::aborted("Raft proposal was superseded"))
        };
        let _ = self.sender.send(result);
    }
}

struct PendingCleanup {
    waiters: Arc<StdMutex<HashMap<LogIndex, PendingProposal>>>,
    index: LogIndex,
    id: u64,
    armed: bool,
}

impl PendingCleanup {
    fn finish(mut self) {
        remove_pending(&self.waiters, self.index, self.id);
        self.armed = false;
    }
}

impl Drop for PendingCleanup {
    fn drop(&mut self) {
        if self.armed {
            remove_pending(&self.waiters, self.index, self.id);
        }
    }
}

pub(crate) struct RaftRuntime {
    config: NodeConfig,
    machine: Mutex<RaftMachine>,
    state: Arc<RwLock<KvState>>,
    waiters: Arc<StdMutex<HashMap<LogIndex, PendingProposal>>>,
    next_proposal_id: AtomicU64,
    transport: PeerTransport,
    failed: AtomicBool,
}

impl RaftRuntime {
    pub(crate) fn open(
        config: NodeConfig,
        state: Arc<RwLock<KvState>>,
    ) -> Result<Self, RuntimeError> {
        validate_config(&config)?;
        let voters = config.peers.keys().copied().collect::<Vec<_>>();
        let initial = RaftNode::new(config.consensus.clone(), &voters);
        let store = RaftStore::open(&config.raft_dir(), initial.persistent().clone())?;
        let node = RaftNode::restore(config.consensus.clone(), store.persistent().clone());
        let transport = PeerTransport::new(config.node_id, config.raft_key.clone(), &config.peers);
        Ok(Self {
            config,
            machine: Mutex::new(RaftMachine { node, store }),
            state,
            waiters: Arc::new(StdMutex::new(HashMap::new())),
            next_proposal_id: AtomicU64::new(1),
            transport,
            failed: AtomicBool::new(false),
        })
    }

    pub(crate) async fn run(self: Arc<Self>) -> Result<(), RuntimeError> {
        loop {
            sleep(TICK_INTERVAL).await;
            self.tick_once().await?;
        }
    }

    pub(crate) async fn step(&self, message: Message) -> Result<(), RuntimeError> {
        self.ensure_healthy()?;
        let mut machine = self.machine.lock().await;
        let effects = machine.node.step(message);
        self.handle_effects(&mut machine, effects).await.map_err(|error| self.fail(error))
    }

    pub(crate) fn validate_message(&self, message: &Message) -> Result<(), MessageError> {
        if message.to != self.config.node_id {
            return Err(MessageError::WrongRecipient {
                expected: self.config.node_id,
                actual: message.to,
            });
        }
        if message.from == self.config.node_id {
            return Err(MessageError::SelfMessage);
        }
        if !self.config.peers.contains_key(&message.from) {
            return Err(MessageError::UnknownSender { sender: message.from });
        }
        Ok(())
    }

    pub(crate) fn verify_signature(&self, body: &[u8], signature: &str) -> bool {
        self.config.raft_key.verify(body, signature)
    }

    pub(crate) async fn propose(&self, command: KvCommand) -> Result<KvApplyResult, ConnectError> {
        self.ensure_healthy().map_err(|error| runtime_connect_error(&error))?;
        let proposal_id = self
            .next_proposal_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| ConnectError::resource_exhausted("proposal id space exhausted"))?;
        let bytes = serde_json::to_vec(&command)
            .map_err(|_| ConnectError::internal("failed to encode Raft command"))?;
        let (receiver, cleanup) = {
            let mut machine = self.machine.lock().await;
            let (index, effects) = machine
                .node
                .propose(Command(bytes.clone()))
                .map_err(|error| propose_error(&error))?;
            let (sender, receiver) = oneshot::channel();
            lock_waiters(&self.waiters)
                .insert(index, PendingProposal { id: proposal_id, command: bytes, sender });
            let cleanup = PendingCleanup {
                waiters: self.waiters.clone(),
                index,
                id: proposal_id,
                armed: true,
            };
            if let Err(error) = self.handle_effects(&mut machine, effects).await {
                cleanup.finish();
                let error = self.fail(error);
                return Err(runtime_connect_error(&error));
            }
            (receiver, cleanup)
        };

        let result = match timeout(PROPOSAL_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConnectError::aborted("Raft proposal was dropped")),
            Err(_) => Err(ConnectError::unavailable("Raft proposal timed out")),
        };
        cleanup.finish();
        result
    }

    pub(crate) async fn read_barrier(&self) -> Result<(), ConnectError> {
        match self.propose(KvCommand::ReadBarrier).await? {
            KvApplyResult::ReadBarrier => Ok(()),
            KvApplyResult::RegisterSession(_)
            | KvApplyResult::Put(_)
            | KvApplyResult::Delete(_) => Err(ConnectError::internal("Raft read barrier mismatch")),
        }
    }

    pub(crate) async fn mode(&self) -> String {
        let machine = self.machine.lock().await;
        if machine.node.is_leader() {
            "leader"
        } else if machine.node.is_candidate() {
            "candidate"
        } else if machine.node.is_precandidate() {
            "precandidate"
        } else {
            "follower"
        }
        .to_owned()
    }

    async fn tick_once(&self) -> Result<(), RuntimeError> {
        self.ensure_healthy()?;
        let mut machine = self.machine.lock().await;
        let effects = machine.node.tick();
        self.handle_effects(&mut machine, effects).await.map_err(|error| self.fail(error))
    }

    async fn handle_effects(
        &self,
        machine: &mut RaftMachine,
        effects: Effects,
    ) -> Result<(), RuntimeError> {
        if effects.persist {
            machine.store.save(machine.node.persistent())?;
        }
        if !effects.send_snapshots.is_empty() {
            return Err(RuntimeError::SnapshotTransportUnsupported);
        }
        for message in effects.messages {
            self.send_message(message)?;
        }
        self.apply_committed(&mut machine.node).await;
        Ok(())
    }

    async fn apply_committed(&self, node: &mut RaftNode) {
        loop {
            let Some(entry) = node.committed().next() else {
                return;
            };
            let result = self.apply_command(&entry.command).await;
            node.advance(entry.index);
            self.complete_waiter(entry.index, &entry.command, result);
        }
    }

    async fn apply_command(&self, command: &Command) -> Result<KvApplyResult, ConnectError> {
        let command = serde_json::from_slice(&command.0)
            .map_err(|_| ConnectError::internal("invalid Raft command payload"))?;
        self.state.write().await.apply(command)
    }

    fn complete_waiter(
        &self,
        index: LogIndex,
        command: &Command,
        result: Result<KvApplyResult, ConnectError>,
    ) {
        if let Some(pending) = lock_waiters(&self.waiters).remove(&index) {
            pending.complete(command, result);
        }
    }

    fn send_message(&self, message: Message) -> Result<(), RuntimeError> {
        self.transport.send(message).map_err(|peer| RuntimeError::UnknownPeer { peer })
    }

    fn ensure_healthy(&self) -> Result<(), RuntimeError> {
        if self.failed.load(Ordering::Acquire) { Err(RuntimeError::Failed) } else { Ok(()) }
    }

    fn fail(&self, error: RuntimeError) -> RuntimeError {
        self.failed.store(true, Ordering::Release);
        error
    }
}

fn validate_config(config: &NodeConfig) -> Result<(), RuntimeError> {
    if config.node_id != config.consensus.id {
        return Err(RuntimeError::IdentityMismatch {
            node_id: config.node_id,
            consensus_id: config.consensus.id,
        });
    }
    if config.peers.get(&config.node_id) != Some(&config.raft_addr) {
        return Err(RuntimeError::InvalidSelfPeer {
            node_id: config.node_id,
            raft_addr: config.raft_addr,
        });
    }
    let mut addresses = HashSet::new();
    if let Some(address) = config.peers.values().find(|address| !addresses.insert(**address)) {
        return Err(RuntimeError::DuplicatePeerAddress { address: *address });
    }
    Ok(())
}

fn remove_pending(
    waiters: &StdMutex<HashMap<LogIndex, PendingProposal>>,
    index: LogIndex,
    id: u64,
) {
    let mut waiters = lock_waiters(waiters);
    if waiters.get(&index).is_some_and(|pending| pending.id == id) {
        waiters.remove(&index);
    }
}

fn lock_waiters(
    waiters: &StdMutex<HashMap<LogIndex, PendingProposal>>,
) -> StdMutexGuard<'_, HashMap<LogIndex, PendingProposal>> {
    match waiters.lock() {
        Ok(waiters) => waiters,
        Err(_poisoned) => std::process::abort(),
    }
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

fn runtime_connect_error(error: &RuntimeError) -> ConnectError {
    ConnectError::internal(format!("Raft runtime failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, SocketAddr};

    use cloud9_core::SharedString;
    use cloud9_storage::StorageOptions;

    use crate::RaftKey;

    use super::*;

    #[tokio::test]
    async fn invariant_log_overwrite_cannot_complete_a_different_proposal() {
        let (sender, receiver) = oneshot::channel();
        let pending = PendingProposal { id: 1, command: b"original".to_vec(), sender };

        pending.complete(&Command(b"replacement".to_vec()), Ok(KvApplyResult::ReadBarrier));

        assert!(receiver.await.unwrap().is_err());
    }

    #[test]
    fn invariant_runtime_identity_is_unambiguous() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.consensus.id = NodeId(1);
        assert!(matches!(validate_config(&config), Err(RuntimeError::IdentityMismatch { .. })));

        let mut config = test_config(dir.path());
        config.raft_addr.set_port(19_092);
        assert!(matches!(validate_config(&config), Err(RuntimeError::InvalidSelfPeer { .. })));

        let mut config = test_config(dir.path());
        config.peers.insert(NodeId(1), config.raft_addr);
        assert!(matches!(validate_config(&config), Err(RuntimeError::DuplicatePeerAddress { .. })));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_cancelled_delivery_does_not_reapply_a_command() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let state = Arc::new(RwLock::new(KvState::new()));
        let runtime = Arc::new(RaftRuntime::open(config, state.clone()).unwrap());
        let driver = tokio::spawn(runtime.clone().run());
        timeout(Duration::from_secs(1), async {
            while runtime.mode().await != "leader" {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let state_guard = state.write().await;
        let proposing = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.propose(KvCommand::RegisterSession).await }
        });
        timeout(Duration::from_secs(1), async {
            loop {
                if !runtime.waiters.lock().unwrap().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let waiters = runtime.waiters.clone();
        let (locked_sender, locked_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let lock_thread = std::thread::spawn(move || {
            let _guard = lock_waiters(&waiters);
            locked_sender.send(()).unwrap();
            release_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        });
        locked_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(state_guard);
        let applied_guard = timeout(Duration::from_secs(1), state.read()).await.unwrap();

        proposing.abort();
        drop(applied_guard);
        release_sender.send(()).unwrap();
        lock_thread.join().unwrap();
        let _ = proposing.await;
        assert!(runtime.waiters.lock().unwrap().is_empty());

        let result = runtime.propose(KvCommand::RegisterSession).await.unwrap();
        let KvApplyResult::RegisterSession(response) = result else {
            panic!("session proposal returned the wrong result");
        };
        assert_eq!(2, response.client_id);
        driver.abort();
    }

    fn test_config(path: &std::path::Path) -> NodeConfig {
        let node_id = NodeId(0);
        let raft_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 19_091));
        NodeConfig {
            node_id,
            client_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 19_090)),
            raft_addr,
            peers: BTreeMap::from([(node_id, raft_addr)]),
            raft_key: RaftKey::from_hex(&"01".repeat(32)).unwrap(),
            storage: StorageOptions {
                name: SharedString::literal("test"),
                data_dir: SharedString::from(path.to_string_lossy()),
            },
            consensus: crate::raft_config(node_id),
        }
    }
}

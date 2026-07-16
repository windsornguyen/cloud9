//! Async driver for the pure Raft state machine.
//!
//! Every step is serialized with its WAL. Persistent state is synced before
//! network effects are released, then committed commands are applied in order.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use cloud9_raft::raft::{Effects, Message};
use cloud9_raft::{Command, LogIndex, NodeId, ProposeError, RaftNode};
use connectrpc::ConnectError;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio::time::{Duration, sleep, timeout};
use tracing::warn;

use crate::command::{KvApplyResult, KvCommand, KvState};
use crate::config::NodeConfig;
use crate::store::{RaftStore, StoreError};
use crate::transport::post_raft_message;

const TICK_INTERVAL: Duration = Duration::from_millis(1);
const PROPOSAL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub(crate) enum RuntimeError {
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

pub(crate) struct RaftRuntime {
    config: NodeConfig,
    machine: Mutex<RaftMachine>,
    state: Arc<RwLock<KvState>>,
    waiters: Mutex<HashMap<LogIndex, oneshot::Sender<Result<KvApplyResult, ConnectError>>>>,
    failed: AtomicBool,
}

impl RaftRuntime {
    pub(crate) fn open(
        config: NodeConfig,
        state: Arc<RwLock<KvState>>,
    ) -> Result<Self, RuntimeError> {
        let voters = config.peers.keys().copied().collect::<Vec<_>>();
        let initial = RaftNode::new(config.consensus.clone(), &voters);
        let store = RaftStore::open(&config.raft_dir(), initial.persistent().clone())?;
        let node = RaftNode::restore(config.consensus.clone(), store.persistent().clone());
        Ok(Self {
            config,
            machine: Mutex::new(RaftMachine { node, store }),
            state,
            waiters: Mutex::new(HashMap::new()),
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

    pub(crate) async fn propose(&self, command: KvCommand) -> Result<KvApplyResult, ConnectError> {
        self.ensure_healthy().map_err(|error| runtime_connect_error(&error))?;
        let bytes = serde_json::to_vec(&command)
            .map_err(|_| ConnectError::internal("failed to encode Raft command"))?;
        let (index, receiver) = {
            let mut machine = self.machine.lock().await;
            let (index, effects) =
                machine.node.propose(Command(bytes)).map_err(|error| propose_error(&error))?;
            let (sender, receiver) = oneshot::channel();
            self.waiters.lock().await.insert(index, sender);
            if let Err(error) = self.handle_effects(&mut machine, effects).await {
                self.waiters.lock().await.remove(&index);
                let error = self.fail(error);
                return Err(runtime_connect_error(&error));
            }
            (index, receiver)
        };

        match timeout(PROPOSAL_TIMEOUT, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConnectError::aborted("Raft proposal was dropped")),
            Err(_) => {
                self.waiters.lock().await.remove(&index);
                Err(ConnectError::unavailable("Raft proposal timed out"))
            }
        }
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

    fn send_message(&self, message: Message) -> Result<(), RuntimeError> {
        let addr = self
            .config
            .peers
            .get(&message.to)
            .copied()
            .ok_or(RuntimeError::UnknownPeer { peer: message.to })?;
        tokio::spawn(async move {
            if let Err(error) = post_raft_message(addr, &message).await {
                warn!(%error, to = message.to.0, "failed to send Raft message");
            }
        });
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), RuntimeError> {
        if self.failed.load(Ordering::Acquire) { Err(RuntimeError::Failed) } else { Ok(()) }
    }

    fn fail(&self, error: RuntimeError) -> RuntimeError {
        self.failed.store(true, Ordering::Release);
        error
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

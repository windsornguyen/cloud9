use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use cloud9_core::SharedString;
use cloud9_raft::NodeId;
use cloud9_storage::StorageOptions;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::RaftKey;
use crate::command::{KvApplyResult, KvCommand, KvName, KvState, MAX_VALUE_BYTES};
use crate::config::NodeConfig;
use crate::runtime::RaftRuntime;
use crate::service::{KvApi, kv_app};
use crate::transport::raft_app;

#[test]
fn invariant_committed_failure_is_idempotent() -> Result<()> {
    let mut state = KvState::new();
    let client_one = registered_client(&mut state)?;
    let client_two = registered_client(&mut state)?;
    state.apply(put_command(client_one, 1, false))?;

    assert!(state.apply(put_command(client_one, 2, true)).is_err());
    state.apply(KvCommand::Delete {
        client_id: client_two,
        sequence: 1,
        namespace: "test".to_owned(),
        key: "key".to_owned(),
        if_match: String::new(),
    })?;

    assert!(state.apply(put_command(client_one, 2, true)).is_err());
    assert!(!state.entries.contains_key(&KvName::new("test", "key")?));
    Ok(())
}

#[test]
fn invariant_oversized_values_never_enter_the_state_machine() -> Result<()> {
    let mut state = KvState::new();
    let client_id = registered_client(&mut state)?;
    let mut command = put_command(client_id, 1, false);
    if let KvCommand::Put { body, .. } = &mut command {
        *body = vec![0; MAX_VALUE_BYTES + 1];
    }

    assert!(state.apply(command).is_err());
    assert!(state.entries.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invariant_mutation_sequence_identifies_exact_request() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = test_config(dir.path());
    let state = Arc::new(RwLock::new(KvState::new()));
    let runtime = Arc::new(RaftRuntime::open(config.clone(), state.clone())?);
    let api = Arc::new(KvApi::new(config, state, runtime.clone()));
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(axum::serve(listener, kv_app(api)).into_future());
    let driver = tokio::spawn(runtime.run());
    wait_for_leader(addr).await?;

    let (status, body) = post_json(addr, "RegisterSession", "{}")?;
    assert_eq!(200, status);
    assert!(body.contains("\"clientId\":\"1\""));

    let put_one = r#"{"clientId":"1","sequence":"1","namespace":"jepsen","key":"register","body":"MQ==","ifNoneMatch":true}"#;
    assert_eq!(200, post_json(addr, "Put", put_one)?.0);
    assert_eq!(200, post_json(addr, "Put", put_one)?.0);

    let changed = r#"{"clientId":"1","sequence":"1","namespace":"jepsen","key":"register","body":"Mg==","ifNoneMatch":true}"#;
    assert_eq!(409, post_json(addr, "Put", changed)?.0);

    let create_again = r#"{"clientId":"1","sequence":"2","namespace":"jepsen","key":"register","body":"Mg==","ifNoneMatch":true}"#;
    assert_eq!(400, post_json(addr, "Put", create_again)?.0);

    let replace = r#"{"clientId":"1","sequence":"3","namespace":"jepsen","key":"register","body":"Mg==","ifMatch":"\"c9-1\""}"#;
    assert_eq!(200, post_json(addr, "Put", replace)?.0);

    let (status, body) = post_json(addr, "Get", r#"{"namespace":"jepsen","key":"register"}"#)?;
    assert_eq!(200, status);
    assert!(body.contains("\"body\":\"Mg==\""));

    let stale_etag = r#"{"clientId":"1","sequence":"4","namespace":"jepsen","key":"register","body":"Mw==","ifMatch":"\"c9-1\""}"#;
    assert_eq!(400, post_json(addr, "Put", stale_etag)?.0);

    server.abort();
    driver.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invariant_restart_recovers_committed_state() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config = test_config(dir.path());
    let state = Arc::new(RwLock::new(KvState::new()));
    let runtime = Arc::new(RaftRuntime::open(config.clone(), state.clone())?);
    let driver = tokio::spawn(runtime.clone().run());
    wait_for_runtime_leader(&runtime).await?;

    let client_id = match runtime.propose(KvCommand::RegisterSession).await? {
        KvApplyResult::RegisterSession(response) => response.client_id,
        KvApplyResult::Put(_) | KvApplyResult::Delete(_) | KvApplyResult::ReadBarrier => {
            bail!("session proposal returned the wrong result")
        }
    };
    runtime
        .propose(KvCommand::Put {
            client_id,
            sequence: 1,
            namespace: "test".to_owned(),
            key: "key".to_owned(),
            body: b"value".to_vec(),
            if_match: String::new(),
            if_none_match: false,
        })
        .await?;

    driver.abort();
    let _ = driver.await;
    drop(runtime);
    drop(state);

    let recovered_state = Arc::new(RwLock::new(KvState::new()));
    let recovered = Arc::new(RaftRuntime::open(config, recovered_state.clone())?);
    let recovered_driver = tokio::spawn(recovered.clone().run());
    wait_for_runtime_leader(&recovered).await?;
    recovered.read_barrier().await?;

    let state = recovered_state.read().await;
    let record = state
        .entries
        .get(&KvName::new("test", "key")?)
        .ok_or_else(|| anyhow::anyhow!("recovered key is missing"))?;
    assert_eq!(b"value", record.body.as_slice());

    recovered_driver.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invariant_three_nodes_replicate_and_fail_over() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?);
    }
    let peers = listeners
        .iter()
        .enumerate()
        .map(|(id, listener)| Ok((NodeId(u64::try_from(id)?), listener.local_addr()?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let key = RaftKey::from_hex(&"01".repeat(32))?;
    let mut states = Vec::new();
    let mut runtimes = Vec::new();
    let mut servers = Vec::new();
    let mut drivers = Vec::new();

    for (id, listener) in listeners.into_iter().enumerate() {
        let node_id = NodeId(u64::try_from(id)?);
        let state = Arc::new(RwLock::new(KvState::new()));
        let config = NodeConfig {
            node_id,
            client_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            raft_addr: listener.local_addr()?,
            peers: peers.clone(),
            raft_key: key.clone(),
            storage: StorageOptions {
                name: SharedString::literal("test"),
                data_dir: SharedString::from(dir.path().join(id.to_string()).to_string_lossy()),
            },
            consensus: crate::raft_config(node_id),
        };
        let runtime = Arc::new(RaftRuntime::open(config, state.clone())?);
        servers.push(tokio::spawn(axum::serve(listener, raft_app(runtime.clone())).into_future()));
        drivers.push(tokio::spawn(runtime.clone().run()));
        states.push(state);
        runtimes.push(runtime);
    }

    let leader = wait_for_cluster_leader(&runtimes, &[0, 1, 2]).await?;
    let client_id = registered_session(&runtimes[leader]).await?;
    runtimes[leader].propose(put_command(client_id, 1, false)).await?;
    wait_for_replicated_key(&states).await?;

    drivers[leader].abort();
    servers[leader].abort();
    let survivors = (0..3).filter(|node| *node != leader).collect::<Vec<_>>();
    let replacement = wait_for_cluster_leader(&runtimes, &survivors).await?;
    runtimes[replacement].read_barrier().await?;

    for task in drivers {
        task.abort();
    }
    for task in servers {
        task.abort();
    }
    Ok(())
}

fn test_config(path: &std::path::Path) -> NodeConfig {
    let node_id = cloud9_raft::NodeId(0);
    let raft_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 19_091));
    NodeConfig {
        node_id,
        client_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 19_090)),
        raft_addr,
        peers: std::collections::BTreeMap::from([(node_id, raft_addr)]),
        raft_key: RaftKey::from_hex(&"01".repeat(32)).unwrap(),
        storage: StorageOptions {
            name: SharedString::literal("test"),
            data_dir: SharedString::from(path.to_string_lossy()),
        },
        consensus: crate::raft_config(node_id),
    }
}

fn registered_client(state: &mut KvState) -> Result<u64> {
    match state.apply(KvCommand::RegisterSession)? {
        KvApplyResult::RegisterSession(response) => Ok(response.client_id),
        KvApplyResult::Put(_) | KvApplyResult::Delete(_) | KvApplyResult::ReadBarrier => {
            bail!("session command returned the wrong result")
        }
    }
}

async fn registered_session(runtime: &RaftRuntime) -> Result<u64> {
    match runtime.propose(KvCommand::RegisterSession).await? {
        KvApplyResult::RegisterSession(response) => Ok(response.client_id),
        KvApplyResult::Put(_) | KvApplyResult::Delete(_) | KvApplyResult::ReadBarrier => {
            bail!("session proposal returned the wrong result")
        }
    }
}

fn put_command(client_id: u64, sequence: u64, if_none_match: bool) -> KvCommand {
    KvCommand::Put {
        client_id,
        sequence,
        namespace: "test".to_owned(),
        key: "key".to_owned(),
        body: b"value".to_vec(),
        if_match: String::new(),
        if_none_match,
    }
}

async fn wait_for_runtime_leader(runtime: &RaftRuntime) -> Result<()> {
    for _ in 0..100 {
        if runtime.mode().await == "leader" {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("single-node Raft runtime did not elect a leader")
}

async fn wait_for_cluster_leader(runtimes: &[Arc<RaftRuntime>], nodes: &[usize]) -> Result<usize> {
    for _ in 0..500 {
        let mut leader = None;
        for node in nodes {
            if runtimes[*node].mode().await == "leader" && leader.replace(*node).is_some() {
                leader = None;
                break;
            }
        }
        if let Some(leader) = leader {
            return Ok(leader);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("Raft cluster did not elect exactly one leader")
}

async fn wait_for_replicated_key(states: &[Arc<RwLock<KvState>>]) -> Result<()> {
    let name = KvName::new("test", "key")?;
    for _ in 0..200 {
        let mut present = true;
        for state in states {
            present &= state.read().await.entries.contains_key(&name);
        }
        if present {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("committed key did not reach every state machine")
}

async fn wait_for_leader(addr: SocketAddr) -> Result<()> {
    for _ in 0..100 {
        let (status, body) = post_json(addr, "Status", "{}")?;
        if status == 200 && body.contains("\"mode\":\"leader\"") {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    bail!("single-node Raft runtime did not elect a leader")
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
    let Some(status) = head.lines().next().and_then(|line| line.split_whitespace().nth(1)) else {
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

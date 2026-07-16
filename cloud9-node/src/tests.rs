use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use cloud9_core::SharedString;
use cloud9_storage::StorageOptions;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::command::{KvApplyResult, KvCommand, KvName, KvState};
use crate::config::NodeConfig;
use crate::runtime::RaftRuntime;
use crate::service::{KvApi, kv_app};

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

fn test_config(path: &std::path::Path) -> NodeConfig {
    NodeConfig {
        storage: StorageOptions {
            name: SharedString::literal("test"),
            data_dir: SharedString::from(path.to_string_lossy()),
        },
        ..NodeConfig::default()
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

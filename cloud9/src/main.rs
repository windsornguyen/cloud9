#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use cloud9_core::{SharedString, fs, install_diagnostics};
use cloud9_node::{NodeConfig, raft_config};
use cloud9_raft::NodeId;
use cloud9_storage::StorageOptions;
use miette::{Context, IntoDiagnostic, Result};
use serde::Deserialize;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(author, version, about = "Cloud9 database daemon", propagate_version = true)]
struct Cli {
    /// Increase logging verbosity (`-vv` enables trace-level logs).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
    /// Disable ANSI colors in log output.
    #[arg(long, default_value_t = false)]
    no_color: bool,
    /// Disable rendering of progress indicators.
    #[arg(long, default_value_t = false)]
    no_progress: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Boot the Cloud9 node using a configuration file.
    Start {
        /// Path to the config file.
        #[arg(long, default_value = "cloud9.toml")]
        config: PathBuf,
    },
    /// Validate the current configuration and exit.
    CheckConfig {
        /// Path to the config file.
        #[arg(long, default_value = "cloud9.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    install_diagnostics()?;

    let cli = Cli::parse();
    init_tracing(cli.verbose, !cli.no_color)?;
    if cli.no_progress {
        tracing::debug!("progress disabled for this run");
    }

    match cli.command {
        Command::Start { config } => {
            let node_config = load_node_config(&config)?;
            tracing::info!(path = %config.display(), "booting node");
            cloud9_node::launch(node_config).await.map_err(|error| miette::miette!("{error:#}"))?;
        }
        Command::CheckConfig { config } => {
            load_node_config(&config).context("configuration check failed")?;
            tracing::info!(path = %config.display(), "configuration OK");
        }
    }

    Ok(())
}

fn init_tracing(verbosity: u8, color_enabled: bool) -> Result<()> {
    let default_filter = if verbosity == 0 {
        "info"
    } else if verbosity == 1 {
        "cloud9=debug"
    } else {
        "cloud9=trace"
    };
    let filter = if std::env::var_os("RUST_LOG").is_some() {
        let value = std::env::var("RUST_LOG").into_diagnostic()?;
        EnvFilter::try_new(value).into_diagnostic()?
    } else {
        EnvFilter::new(default_filter)
    };

    let fmt_layer = fmt::layer()
        .with_target(verbosity > 0)
        .with_ansi(color_enabled)
        .with_timer(fmt::time::UtcTime::rfc_3339());

    tracing_subscriber::registry().with(filter).with(fmt_layer).try_init().into_diagnostic()
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    node: NodeSection,
    storage: StorageSection,
    cluster: ClusterSection,
}

#[derive(Debug, Deserialize)]
struct NodeSection {
    id: u64,
    #[serde(rename = "host")]
    _host: String,
    client_port: u16,
    raft_port: u16,
}

#[derive(Debug, Deserialize)]
struct StorageSection {
    data_dir: String,
}

#[derive(Debug, Deserialize)]
struct ClusterSection {
    peers: Vec<PeerSection>,
}

#[derive(Debug, Deserialize)]
struct PeerSection {
    id: u64,
    host: String,
    raft_port: u16,
}

fn load_node_config(path: &Path) -> Result<NodeConfig> {
    let contents = fs::read_to_string(path)
        .into_diagnostic()
        .with_context(|| format!("reading configuration from `{}`", path.display()))?;
    parse_node_config(&contents).with_context(|| format!("parsing `{}`", path.display()))
}

fn parse_node_config(contents: &str) -> Result<NodeConfig> {
    let config: ConfigFile = toml::from_str(contents).into_diagnostic()?;
    let node_id = NodeId(config.node.id);
    let client_addr = bind_addr(config.node.client_port);
    let raft_addr = bind_addr(config.node.raft_port);
    let peers = peer_addrs(&config.cluster.peers)?;
    if !peers.contains_key(&node_id) {
        return Err(miette::miette!("cluster.peers must include node.id {}", node_id.0));
    }

    Ok(NodeConfig {
        node_id,
        client_addr,
        raft_addr,
        peers,
        storage: StorageOptions {
            name: SharedString::from("default"),
            data_dir: SharedString::from(config.storage.data_dir),
        },
        consensus: raft_config(node_id),
    })
}

fn bind_addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::UNSPECIFIED, port))
}

fn peer_addrs(peers: &[PeerSection]) -> Result<BTreeMap<NodeId, SocketAddr>> {
    peers
        .iter()
        .map(|peer| Ok((NodeId(peer.id), resolve_peer_addr(&peer.host, peer.raft_port)?)))
        .collect()
}

fn resolve_peer_addr(host: &str, port: u16) -> Result<SocketAddr> {
    let addrs = (host, port)
        .to_socket_addrs()
        .into_diagnostic()
        .with_context(|| format!("resolving peer address `{host}:{port}`"))?
        .collect::<Vec<_>>();
    match addrs.as_slice() {
        [addr] => Ok(*addr),
        [] => Err(miette::miette!("peer address `{host}:{port}` resolved to no addresses")),
        _ => Err(miette::miette!(
            "peer address `{host}:{port}` resolved ambiguously to {} addresses",
            addrs.len()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invariant_startup_requires_a_configuration_file() {
        let dir = tempfile::tempdir().into_diagnostic().unwrap();
        let error = load_node_config(&dir.path().join("missing.toml")).unwrap_err();

        assert!(error.to_string().contains("missing"));
    }
}

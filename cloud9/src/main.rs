use std::path::PathBuf;

use clap::{Parser, Subcommand};
use cloud9_core::{fs, install_diagnostics};
use miette::{Context, IntoDiagnostic, Result};
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
    /// Boot the Cloud9 node using an optional configuration file.
    Start {
        /// Optional path to a configuration file (defaults to `cloud9.toml`).
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Validate the current configuration and exit.
    CheckConfig {
        /// Path to the config file.
        #[arg(long, default_value = "cloud9.toml")]
        config: PathBuf,
    },
}

fn main() -> Result<()> {
    install_diagnostics()?;

    let cli = Cli::parse();
    init_tracing(cli.verbose, !cli.no_color)?;

    match cli.command {
        Command::Start { config } => {
            let config_path = config.unwrap_or_else(|| PathBuf::from("cloud9.toml"));
            let maybe_config = load_config(&config_path)?;
            tracing::info!(path = %config_path.display(), "booting node");
            if let Some(config) = maybe_config {
                tracing::debug!(contents = %config, "loaded configuration");
            } else {
                tracing::warn!(path = %config_path.display(), "using defaults; config missing");
            }
        }
        Command::CheckConfig { config } => {
            load_config(&config)
                .and_then(|contents| {
                    contents.ok_or_else(|| miette::miette!("config `{}` missing", config.display()))
                })
                .context("configuration check failed")?;
            tracing::info!(path = %config.display(), "configuration OK");
        }
    }

    if cli.no_progress {
        tracing::debug!("progress disabled for this run");
    }

    Ok(())
}

fn init_tracing(verbosity: u8, color_enabled: bool) -> Result<()> {
    let filter = if verbosity == 0 {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    } else if verbosity == 1 {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("cloud9=debug"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("cloud9=trace"))
    };

    let fmt_layer = fmt::layer()
        .with_target(verbosity > 0)
        .with_ansi(color_enabled)
        .with_timer(fmt::time::UtcTime::rfc_3339());

    tracing_subscriber::registry().with(filter).with(fmt_layer).try_init().into_diagnostic()
}

fn load_config(path: &PathBuf) -> Result<Option<String>> {
    if path.exists() {
        fs::read_to_string(path)
            .into_diagnostic()
            .map(Some)
            .with_context(|| format!("reading configuration from `{}`", path.display()))
    } else {
        Ok(None)
    }
}

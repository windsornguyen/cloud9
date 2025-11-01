//! Storage primitives for Cloud9.

use cloud9_core::SharedString;
use serde::{Deserialize, Serialize};

/// Storage engine tuning parameters surfaced to higher layers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageOptions {
    /// Human-readable identifier for the backing store.
    pub name: SharedString,
    /// Path to the underlying data directory.
    pub data_dir: SharedString,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            name: SharedString::literal("default"),
            data_dir: SharedString::literal("/var/lib/cloud9"),
        }
    }
}

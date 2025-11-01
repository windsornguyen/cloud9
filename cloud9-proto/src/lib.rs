//! Protocol-level data structures shared between services.

use cloud9_core::SharedString;
use serde::{Deserialize, Serialize};

/// Identifies a tenant within the global system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TenantId(pub SharedString);

/// A thin wrapper for user-supplied database names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DatabaseName(pub SharedString);

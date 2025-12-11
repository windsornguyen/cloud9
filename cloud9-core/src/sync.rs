//! Synchronization primitives with loom support.
//!
//! This module provides a unified interface for synchronization primitives that
//! work with both standard library types and loom's testing equivalents.
//!
//! When compiled with `cfg(loom)`, this module re-exports loom's primitives,
//! enabling exhaustive concurrency testing. Otherwise, it re-exports std.
//!
//! # Usage
//!
//! ```ignore
//! use cloud9_core::sync::{Arc, atomic::AtomicU64};
//! use cloud9_core::sync::atomic::Ordering;
//!
//! let counter = Arc::new(AtomicU64::new(0));
//! counter.fetch_add(1, Ordering::SeqCst);
//! ```
//!
//! # Testing with Loom
//!
//! Run loom tests with:
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test --release --lib loom
//! ```

#[cfg(loom)]
pub use loom::sync::*;

#[cfg(loom)]
pub use loom::thread;

#[cfg(loom)]
pub use loom::cell;

#[cfg(loom)]
pub use loom::hint;

#[cfg(loom)]
pub use loom::lazy_static;

#[cfg(not(loom))]
pub use std::sync::*;

#[cfg(not(loom))]
pub use std::thread;

/// Atomic primitives.
pub mod atomic {
    #[cfg(loom)]
    pub use loom::sync::atomic::*;

    #[cfg(not(loom))]
    pub use std::sync::atomic::*;
}

/// Run a loom model check.
///
/// In non-loom builds, this simply executes the closure once.
/// In loom builds, this explores all possible thread interleavings.
///
/// # Example
///
/// ```ignore
/// use cloud9_core::sync::{Arc, atomic::AtomicUsize, atomic::Ordering, model};
///
/// #[test]
/// fn test_atomic_increment() {
///     model(|| {
///         let x = Arc::new(AtomicUsize::new(0));
///         // ... concurrent operations
///     });
/// }
/// ```
#[cfg(loom)]
pub fn model<F>(f: F)
where
    F: Fn() + Sync + Send + 'static,
{
    loom::model(f);
}

#[cfg(not(loom))]
pub fn model<F>(f: F)
where
    F: FnOnce(),
{
    f();
}

/// Cell types for interior mutability under loom.
#[cfg(not(loom))]
pub mod cell {
    pub use std::cell::*;
}

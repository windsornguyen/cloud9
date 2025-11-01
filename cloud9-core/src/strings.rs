//! Shared string types used across the workspace.

use std::borrow::Cow;
use std::ops::Deref;

/// Compact, reference-counted string inspired by uv's `SmallString`.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SharedString(arcstr::ArcStr);

impl SharedString {
    /// Borrow the underlying string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Create a literal without allocating at runtime.
    #[inline]
    pub fn literal(value: &'static str) -> Self {
        Self(arcstr::ArcStr::from(value))
    }
}

impl Deref for SharedString {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for SharedString {
    #[inline]
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for SharedString {
    #[inline]
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<arcstr::ArcStr> for SharedString {
    #[inline]
    fn from(value: arcstr::ArcStr) -> Self {
        Self(value)
    }
}

impl From<Cow<'_, str>> for SharedString {
    fn from(value: Cow<'_, str>) -> Self {
        match value {
            Cow::Borrowed(inner) => Self::from(inner),
            Cow::Owned(inner) => Self::from(inner),
        }
    }
}

impl AsRef<str> for SharedString {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

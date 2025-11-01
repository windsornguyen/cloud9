//! Thin wrappers around filesystem operations that enforce the `fs_err` error context.

use std::borrow::Cow;
use std::io;
use std::path::{Path, PathBuf};

/// Append an extension without clobbering an existing suffix.
///
/// This is lifted straight from `uv`'s `with_added_extension` helper so that we can depend on the
/// same semantics when manipulating staged files.
pub fn with_added_extension<'a>(path: &'a Path, extension: &str) -> Cow<'a, Path> {
    let Some(name) = path.file_name() else {
        return Cow::Borrowed(path);
    };
    let mut name = name.to_os_string();
    name.push(".");
    name.push(extension.trim_start_matches('.'));
    Cow::Owned(path.with_file_name(name))
}

/// Mirrors `std::fs::create_dir_all` but ensures rich I/O errors via `fs_err`.
#[inline]
pub fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    fs_err::create_dir_all(path)
}

/// Mirrors `std::fs::remove_dir_all` with consistent error handling.
#[inline]
pub fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
    fs_err::remove_dir_all(path)
}

/// Read a UTF-8 file, returning the same errors as `fs_err::read_to_string`.
#[inline]
pub fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
    fs_err::read_to_string(path)
}

/// Write a UTF-8 buffer to disk.
#[inline]
pub fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
    fs_err::write(path, contents)
}

/// Resolve a path with proper I/O context.
#[inline]
pub fn canonicalize(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    fs_err::canonicalize(path)
}

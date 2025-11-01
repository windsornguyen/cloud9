//! Consistent error reporting and tracing setup shared across binaries.

use miette::Result;
use textwrap::{WordSeparator, WordSplitter};

/// Install the global [`miette`] report handler so all crates render diagnostics the same way.
///
/// This mirrors the approach used by the `uv` toolchain, giving us opinionated defaults while
/// keeping the hook installable from any binary early in `main`.
pub fn install_diagnostics() -> Result<()> {
    miette::set_hook(Box::new(|_| {
        Box::new(
            miette::MietteHandlerOpts::new()
                .wrap_lines(true)
                .break_words(false)
                .word_separator(WordSeparator::AsciiSpace)
                .word_splitter(WordSplitter::NoHyphenation)
                .build(),
        )
    }))?;

    Ok(())
}

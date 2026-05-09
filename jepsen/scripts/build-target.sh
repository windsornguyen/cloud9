#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"

cargo build --release -p cloud9 --bin c9 --locked

printf '  ok  c9 (%s)\n' "$repo/target/release/c9"

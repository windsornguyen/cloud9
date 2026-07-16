#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo"

if [[ "$(uname -s)" != Linux ]]; then
  printf 'error: build c9 on a Linux host matching the Jepsen DB nodes\n' >&2
  exit 1
fi

cargo build --release -p cloud9 --bin c9 --locked

printf '  ok  c9 (%s)\n' "$repo/target/release/c9"

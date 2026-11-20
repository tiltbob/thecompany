#!/usr/bin/env bash
# Runs every check CI runs, in CI's order, and stops at the first failure. CI runs the same
# commands as separate steps in .github/workflows/ci.yml; keep the two in step. Works from any
# directory.
set -euo pipefail
cd "$(dirname "$0")/.."

err() {
    echo "FAILED: $*" >&2
    exit 1
}

clippy_out=$(mktemp)
trap 'rm -f "$clippy_out"' EXIT

# run <name> <command...>
run() {
    local name=$1
    shift
    echo "==> $name: $*"
    "$@" || err "$name"
    echo "ok: $name"
}

# Clippy's `-D warnings` does not reach its own configuration warnings (for example a path in
# clippy.toml that does not exist), so any line starting with `warning` fails the check too.
clippy() {
    local status=0
    cargo clippy --workspace --all-targets --locked --color never -- -D warnings 2>&1 \
        | tee "$clippy_out" || status=$?
    [[ $status == 0 ]] || return 1
    ! grep -q '^warning' "$clippy_out"
}

run format cargo fmt --all --check
run clippy clippy
run build cargo build --workspace --locked
run test cargo test --workspace --locked
run "numbers features" cargo check -p numbers --locked
run "dependency guards" bash scripts/check-deps.sh
run "lint guards" bash scripts/check-lints.sh
run docs env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
run "systemd units" bash scripts/check-units.sh

echo "all checks passed"

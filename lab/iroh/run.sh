#!/bin/sh
# Reruns everything: format and lint checks, the verbatim-snippet build, the whole test suite
# (T0-T14, ~60-90 s, tests run in parallel), then the suite again inside a network namespace that
# has only loopback (T13). MEASURE lines go to results.txt and results-netns.txt.
set -eu
cd "$(dirname "$0")"
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --features verbatim
cargo test --locked -- --nocapture 2>&1 | tee results.txt | grep -E 'MEASURE|^test |test result'
! grep -q 'FAILED' results.txt
# T13: loopback-only network namespace. `ip` is not installed here, so lo is brought up by
# scripts/lo-up.py (an ioctl); with iproute2 use `ip link set lo up` instead.
unshare -n sh -c 'python3 -I scripts/lo-up.py && cargo test --locked --offline -- --nocapture' \
  2>&1 | tee results-netns.txt | grep -E 'MEASURE|^test |test result'
! grep -q 'FAILED' results-netns.txt

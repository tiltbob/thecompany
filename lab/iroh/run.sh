#!/bin/sh
# Reruns everything: format and lint checks, the verbatim-snippet build, the M1 suite (tests/lab.rs,
# T0-T14, ~60-90 s) and the M2 suite (tests/m2.rs, T20-T27, ~2 min), then both suites again inside
# a network namespace that has only loopback (T13). Tests in a suite run in parallel. MEASURE lines
# go to results.txt / results-m2.txt and results-netns.txt / results-m2-netns.txt.
set -eu
cd "$(dirname "$0")"
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked --features verbatim
cargo test --locked --test lab -- --nocapture 2>&1 | tee results.txt | grep -E 'MEASURE|^test |test result'
! grep -q 'FAILED' results.txt
cargo test --locked --test m2 -- --nocapture 2>&1 | tee results-m2.txt | grep -E 'MEASURE|^test |test result'
! grep -q 'FAILED' results-m2.txt
# T13: loopback-only network namespace. `ip` is not installed here, so lo is brought up by
# scripts/lo-up.py (an ioctl); with iproute2 use `ip link set lo up` instead. Nothing in either
# suite may need more than loopback: no public DNS (T26 uses its own DNS server on 127.0.0.1).
unshare -n sh -c 'python3 -I scripts/lo-up.py && cargo test --locked --offline --test lab -- --nocapture' \
  2>&1 | tee results-netns.txt | grep -E 'MEASURE|^test |test result'
! grep -q 'FAILED' results-netns.txt
unshare -n sh -c 'python3 -I scripts/lo-up.py && cargo test --locked --offline --test m2 -- --nocapture' \
  2>&1 | tee results-m2-netns.txt | grep -E 'MEASURE|^test |test result'
! grep -q 'FAILED' results-m2-netns.txt

# iroh lab

A throwaway crate that compiles the code in `docs/notes/iroh-snippets.md` against the iroh and
iroh-relay versions in its own `Cargo.lock`, and measures the behaviour the appendix of
`docs/ARCHITECTURE.md` describes. It is not part of the workspace (it has its own `[workspace]`
table and lock file) and CI does not build it.

Everything runs in-process on 127.0.0.1; `iroh/test-utils` is never enabled. Keys are generated per
run. The result files are ignored by git, because they print endpoint ids.

```bash
cd lab/iroh
./run.sh
```

About 8 minutes. The last steps rerun both suites inside a network namespace that has only
loopback, which needs root for `unshare -n`. `src/verbatim.rs` holds the snippets as written, with
the three fixes they need marked `FIX1` to `FIX3`; `tests/lab.rs` holds experiments T0 to T14 (M1)
and `tests/m2.rs` holds T20 to T27 (M2: a TLS tower from certificate files, QAD, two towers,
runtime relay-map changes, a denying tower, the rate limit, SRV against a fake DNS server on
127.0.0.1, first home tower with several towers); both print one `MEASURE` line per number. The
M2 library side is `src/tls_tower.rs`, `src/allowlist.rs`, `src/locate.rs` and `MapLookup` in
`src/lookup.rs`. `cargo test --features net-report --test m2 t21` also prints iroh's unstable net
report.

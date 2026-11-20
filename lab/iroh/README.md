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

About 4.5 minutes. The last step reruns the suite inside a network namespace that has only
loopback, which needs root for `unshare -n`. `src/verbatim.rs` is generated from the notes: every rust
block of sections 2 to 10, byte for byte, wrapped only in functions that supply the free variables.
Regenerate it after editing the notes and build it with the `verbatim` feature:

```bash
python3 -I scripts/gen-verbatim.py ../../docs/notes/iroh-snippets.md > src/verbatim.rs
cargo build --locked --features verbatim
```

`tests/lab.rs` holds experiments T0 to T14 and prints one `MEASURE` line per number.

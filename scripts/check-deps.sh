#!/usr/bin/env bash
# Dependency guards that `cargo clippy --workspace` cannot see. Run from the repository root.
#
# Hard rule 5 (AGENTS.md): frontends hold no network state. `chief` and `chief-cli` depend on
# `numbers-base` only, never on `numbers` or iroh. Each package is resolved on its own, so workspace
# feature unification cannot hide a dependency.
set -euo pipefail

fail=0

# Prints the names of every crate in the normal and build dependency tree of package $1.
deps_of() {
    cargo tree -p "$1" -e normal,build --locked --prefix none --format '{p}' \
        | awk '{print $1}' | sort -u
}

# forbid <package> <crate>...: fails if any listed crate is in the package's tree.
forbid() {
    local pkg=$1
    shift
    local tree
    tree=$(deps_of "$pkg")
    for crate in "$@"; do
        if grep -qx -- "$crate" <<<"$tree"; then
            echo "error: $pkg depends on $crate" >&2
            cargo tree -p "$pkg" -e normal,build --locked -i "$crate" >&2 || true
            fail=1
        else
            echo "ok: $pkg does not depend on $crate"
        fi
    done
}

for frontend in chief chief-cli; do
    forbid "$frontend" numbers iroh iroh-base iroh-relay
done

# Agent confinement (hard rule 1), from M5: only `safehouse` may pull in PTY or harness crates.
# Add the check here when crates/safehouse lands, e.g.
#   for pkg in numbers-tower numbers-station numbers-station-daemon chief chief-cli; do
#       forbid "$pkg" <pty crate> ...
#   done

exit "$fail"

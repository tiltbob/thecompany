#!/usr/bin/env bash
# Dependency guards that `cargo clippy --workspace` cannot see. Run from the repository root.
#
# Hard rule 5 (AGENTS.md): frontends hold no network state. `chief` and `chief-cli` depend on
# `numbers-base` only, never on `numbers` or iroh.
# docs/ARCHITECTURE.md, appendix "Dependencies": the libraries never enable iroh's `test-utils`,
# and keep `portmapper` and `fast-apple-datapath` out.
#
# Each package is resolved on its own, so workspace feature unification cannot hide a dependency.
set -euo pipefail

# Shared by every cargo tree call:
# - `--target all`: the phone builds of `chief` are not the host target, and a dependency under
#   `[target.'cfg(target_os = "ios")'.dependencies]` is invisible to a host-only tree.
# - `--all-features`: an optional dependency behind a non-default feature is still a dependency.
# - `-e normal,build`: dev-dependencies are allowed on purpose. Tests never ship, so a frontend's
#   tests may use the network library (an integration test against a real station, for example).
tree_args=(-e "normal,build" --target all --all-features --locked)

fail=0

# Prints the names of every crate in the normal and build dependency tree of package $1.
deps_of() {
    cargo tree -p "$1" "${tree_args[@]}" --prefix none --format '{p}' \
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
            cargo tree -p "$pkg" "${tree_args[@]}" -i "$crate" >&2 || true
            fail=1
        else
            echo "ok: $pkg does not depend on $crate"
        fi
    done
}

# forbid_iroh_features <package> <feature>...: fails if the `iroh` package in the package's own
# tree has any listed feature enabled. `--all-features` stays on here too: a non-default feature
# of our own library that switched on `iroh/test-utils` would still be a feature anyone can enable,
# and the design says never.
forbid_iroh_features() {
    local pkg=$1
    shift
    local features
    # awk reads the whole tree (no early exit), so cargo never writes into a closed pipe.
    features=$(cargo tree -p "$pkg" "${tree_args[@]}" --prefix none --format '{p} [{f}]' \
        | awk '$1 == "iroh" && !seen { sub(/^[^[]*\[/, ""); sub(/\].*$/, ""); print; seen = 1 }')
    # Positive control: iroh is always built with tls-ring, so an empty list means the detector
    # is blind, not that the features are clean.
    if ! tr ',' '\n' <<<"$features" | grep -qx tls-ring; then
        echo "error: the guard cannot read iroh's features in $pkg (got '$features'); fix this script" >&2
        fail=1
        return
    fi
    for feature in "$@"; do
        if tr ',' '\n' <<<"$features" | grep -qx -- "$feature"; then
            echo "error: $pkg enables iroh feature $feature (iroh features: $features)" >&2
            fail=1
        else
            echo "ok: $pkg does not enable iroh feature $feature"
        fi
    done
}

# Positive control: if a change in cargo's output format blinded the detector, every check below
# would print "ok". It must see iroh in numbers.
if ! grep -qx iroh <<<"$(deps_of numbers)"; then
    echo "error: the guard cannot see iroh in numbers; fix this script" >&2
    exit 1
fi

for frontend in chief chief-cli; do
    forbid "$frontend" numbers iroh iroh-base iroh-relay
done

for lib in numbers numbers-station; do
    forbid_iroh_features "$lib" test-utils portmapper fast-apple-datapath
    forbid "$lib" portmapper
done

# Agent confinement (hard rule 1), from M5: only `safehouse` may pull in PTY or harness crates.
# Add the check here when crates/safehouse lands, e.g.
#   for pkg in numbers-tower numbers-station numbers-station-daemon chief chief-cli; do
#       forbid "$pkg" <pty crate> ...
#   done

exit "$fail"

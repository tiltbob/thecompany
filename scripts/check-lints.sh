#!/usr/bin/env bash
# Proves that the lints behind hard rules 1 (agent confinement: no process spawning outside
# `safehouse`) and 10 (no `unsafe`, no `unwrap` outside tests) are still switched on. Several small
# changes switch them off while clippy still exits 0: a second clippy config under crates/,
# CLIPPY_CONF_DIR, a typo in a configured path, a crate that stops inheriting the workspace lints,
# an allow attribute. Run from the repository root; needs git and cargo.
set -euo pipefail

fail=0
err() {
    echo "error: $*" >&2
    fail=1
}

# Clippy configs allowed besides the root one. crates/safehouse/clippy.toml joins in M5.
allowed_clippy_configs=()
# Crates allowed not to inherit the workspace lints (paths of their Cargo.toml). None.
allowed_lint_tables=()
# Source files allowed to name the lints below: the one documented exception to hard rule 1,
# in a frontend started by the chief (the git helper). None yet.
allowed_lint_mentions=()

# in_list <item> <list...>
# Callers expand a possibly empty array as ${a[@]+"${a[@]}"}: bash before 4.4 (macOS ships 3.2)
# treats a plain "${a[@]}" of an empty array as unset under `set -u`.
in_list() {
    local item=$1
    shift
    local x
    for x in "$@"; do
        [[ $x == "$item" ]] && return 0
    done
    return 1
}

# section_has <file> <[section]> <line>: whether <line> appears literally, ignoring surrounding
# whitespace, inside the TOML table <[section]>.
section_has() {
    awk -v sect="$2" -v want="$3" '
        { line = $0; gsub(/^[ \t]+|[ \t]+$/, "", line) }
        line ~ /^\[/ { inside = (line == sect); next }
        inside && line == want { found = 1 }
        END { exit !found }
    ' "$1"
}

# 1. One clippy config: clippy uses the first clippy.toml or .clippy.toml walking up from a crate
#    and never merges, so any other file drops the root config for the crates below it.
#    Untracked (but not ignored) files are included so a local run catches them too.
while IFS= read -r f; do
    [[ $f == clippy.toml ]] && continue
    if in_list "$f" ${allowed_clippy_configs[@]+"${allowed_clippy_configs[@]}"}; then
        continue
    fi
    err "$f: a second clippy config replaces the root clippy.toml for every crate below it, which switches off the confinement lint (hard rule 1)"
done < <(git ls-files --cached --others --exclude-standard -- '*clippy.toml' | sort -u)

# 2. CLIPPY_CONF_DIR points clippy at another config.
while IFS= read -r f; do
    err "$f: sets CLIPPY_CONF_DIR, which replaces the root clippy.toml and its confinement lint (hard rule 1)"
done < <(git grep --untracked -l CLIPPY_CONF_DIR -- .cargo .github || true)

# 3. The root configuration itself.
if [[ ! -f clippy.toml ]]; then
    err "clippy.toml: missing; it holds the confinement lint (hard rule 1)"
else
    # An entry counts only on a line of its own that starts with `{`: a commented-out entry still
    # contains the text. Only the std path can be probed below (the tokio one needs tokio's
    # `process` feature, which nothing enables), so this is the one check of the tokio entry.
    for path in 'std::process::Command' 'tokio::process::Command'; do
        if ! grep -qE "^[[:space:]]*\\{[[:space:]]*path = \"$path\"" clippy.toml; then
            err "clippy.toml: no active disallowed-types entry with path = \"$path\" (hard rule 1)"
        fi
    done
    if ! grep -qx 'allow-unwrap-in-tests = true' clippy.toml; then
        err "clippy.toml: allow-unwrap-in-tests = true is missing (hard rule 10)"
    fi
fi
if ! section_has Cargo.toml '[workspace.lints.rust]' 'unsafe_code = "forbid"'; then
    err 'Cargo.toml: [workspace.lints.rust] must have unsafe_code = "forbid" (hard rule 10)'
fi
if ! section_has Cargo.toml '[workspace.lints.clippy]' 'unwrap_used = "deny"'; then
    err 'Cargo.toml: [workspace.lints.clippy] must have unwrap_used = "deny" (hard rule 10)'
fi
if ! section_has Cargo.toml '[workspace.lints.clippy]' 'disallowed_types = "deny"'; then
    err 'Cargo.toml: [workspace.lints.clippy] must have disallowed_types = "deny" (hard rule 1)'
fi

# 4. Every crate inherits the workspace lints.
for manifest in crates/*/Cargo.toml; do
    if in_list "$manifest" ${allowed_lint_tables[@]+"${allowed_lint_tables[@]}"}; then
        continue
    fi
    if ! section_has "$manifest" '[lints]' 'workspace = true'; then
        err "$manifest: needs a [lints] table with workspace = true, or it loses the lints behind hard rules 1 and 10"
    fi
done

# 5. No allow attribute (or other mention) that switches these lints off for an item or a crate.
#    `disallowed_types` is in clippy's style group (and so in clippy::all), `unwrap_used` in
#    restriction. Any mention of these names is refused, whatever the attribute looks like.
#    `allow(warnings)` is not in the list because it cannot reach them: it lowers only lints at
#    warn level, and both are denied by name in Cargo.toml (check 3). The probe below proves it.
pattern='disallowed_types|clippy::(all|style|restriction|unwrap_used)([^a-z_]|$)'
while IFS= read -r hit; do
    file=${hit%%:*}
    if in_list "$file" ${allowed_lint_mentions[@]+"${allowed_lint_mentions[@]}"}; then
        continue
    fi
    err "$hit: names a lint behind hard rules 1 and 10; an allow attribute would switch it off"
done < <(git grep --untracked -nE "$pattern" -- 'crates/*.rs' || true)

# 6. Live probe: clippy must still reject std::process::Command in a workspace crate, on its own
#    (no `-D warnings` on the command line) and under a blanket `allow(warnings)`.
probe_dir=crates/base/tests
probe=$probe_dir/zz_check_lints_probe.rs
if [[ -e $probe ]]; then
    err "$probe exists already; remove it (a leftover of an interrupted run) and run again"
    exit 1
fi
made_dir=0
if [[ ! -d $probe_dir ]]; then
    mkdir -p "$probe_dir"
    made_dir=1
fi
# shellcheck disable=SC2329 # invoked by the EXIT trap
cleanup() {
    rm -f "$probe"
    if [[ $made_dir == 1 ]]; then
        rmdir "$probe_dir" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM
cat >"$probe" <<'EOF'
//! Temporary probe written by scripts/check-lints.sh. It must never be committed.
#![allow(warnings, reason = "the confinement lint must survive a blanket allow")]
#[test]
fn spawns_a_process() {
    let _ = std::process::Command::new("true");
}
EOF
# shellcheck disable=SC2016 # the backticks are literal clippy output
expected='use of a disallowed type `std::process::Command`'
if out=$(cargo clippy -p numbers-base --tests --locked 2>&1); then
    echo "$out" >&2
    err "clippy accepted std::process::Command in numbers-base: the confinement lint is off (hard rule 1)"
elif ! grep -qF -- "$expected" <<<"$out"; then
    echo "$out" >&2
    err "clippy failed on the probe, but not with the disallowed-types error (hard rule 1)"
else
    echo "ok: clippy rejects std::process::Command"
fi

if [[ $fail == 0 ]]; then
    echo "ok: the lints behind hard rules 1 and 10 are switched on"
fi
exit "$fail"

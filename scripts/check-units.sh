#!/usr/bin/env bash
# Runs `systemd-analyze verify` on the systemd units in packaging/systemd/, with ExecStart
# pointing at the binary built from this checkout. Fails on any message about a unit, a warning
# as much as an error. Without systemd-analyze it fails under CI (the environment variable CI is
# set) and is skipped otherwise. Works from any directory; needs cargo.
set -euo pipefail
cd "$(dirname "$0")/.."

unit=packaging/systemd/numbers-tower.service
installed=/usr/local/bin/tower

if ! command -v systemd-analyze >/dev/null 2>&1; then
    if [[ -n ${CI:-} ]]; then
        echo "error: systemd-analyze is not installed" >&2
        exit 1
    fi
    echo "skipped: systemd-analyze is not installed; the units were not verified"
    exit 0
fi

cargo build -p numbers-tower --bin tower --locked --quiet
target_dir=${CARGO_TARGET_DIR:-target}
built=$(cd "$target_dir/debug" && pwd)/tower
[[ -x $built ]] || {
    echo "error: $built was not built" >&2
    exit 1
}

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

# The unit keeps its file name: verify derives the unit's name from it.
count=$(grep -c "^ExecStart=$installed " "$unit" || true)
[[ $count == 1 ]] || {
    echo "error: $unit needs exactly one line starting with ExecStart=$installed" >&2
    exit 1
}
sed "s|^ExecStart=$installed |ExecStart=$built |" "$unit" >"$scratch/$(basename "$unit")"

# How systemd-analyze 255 reports (src/analyze/analyze-verify-util.c): an error, such as an
# ExecStart program that is not executable, gives a non-zero exit status. A syntax warning,
# such as an unknown key or a value it cannot parse ("..., ignoring."), is only printed, unless
# --recursive-errors is given. With --recursive-errors=no a syntax warning in the unit gives a
# non-zero exit status too, and the units it depends on (network-online.target,
# multi-user.target) are not loaded, so warnings about the host's own unit files cannot
# appear. verify needs no running systemd (it starts a manager in test mode) and no root.
#
# verify prints nothing for a clean unit, so any output also fails the check. No message has
# been seen from the host here (systemd 255 with no systemd running); if one shows up in CI,
# filter that exact message below, with a comment saying where it comes from.
status=0
output=$(systemd-analyze verify --recursive-errors=no "$scratch/$(basename "$unit")" 2>&1) ||
    status=$?
if [[ -n $output ]]; then
    printf '%s\n' "$output" >&2
fi
if [[ $status != 0 || -n $output ]]; then
    echo "error: systemd-analyze verify found problems in $unit (exit status $status; line" \
        "numbers are the unit's own)" >&2
    exit 1
fi
echo "ok: $unit ($(systemd-analyze --version | head -n 1))"

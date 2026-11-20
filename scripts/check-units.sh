#!/usr/bin/env bash
# Verify the system tower unit, its certificate drop-in, and the station user unit. Each actual
# check requires exit 0 and no output. Each scope has a minimal preflight: an unavailable scope
# fails CI, but is skipped locally without skipping the other scope. Builds nothing.
set -euo pipefail
cd "$(dirname "$0")/.."

tower=packaging/systemd/numbers-tower.service
station=packaging/systemd/station.service
dropin=packaging/systemd/numbers-tower.service.d/tls-files.conf.example
min_version=250
failed=0

unavailable() {
    if [[ -n ${CI:-} ]]; then
        echo "error: $1" >&2
        failed=1
    else
        echo "skipped: $1"
    fi
}

if ! command -v systemd-analyze >/dev/null 2>&1; then
    unavailable "systemd-analyze is not installed"
    exit "$failed"
fi
if ! version_output=$(systemd-analyze --version); then
    unavailable "systemd-analyze --version failed"
    exit "$failed"
fi
version=$(awk 'NR == 1 { print $2 }' <<<"$version_output")
if [[ ! $version =~ ^[0-9]+$ ]] || ((version < min_version)); then
    unavailable "systemd-analyze is version ${version:-unknown}; the check needs $min_version or later"
    exit "$failed"
fi

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

# Verify only needs an executable. Each binary's tests check its shipped ExecStart arguments.
# Quote the stub path for spaces and escape '%' specifiers. systemd 255 refuses executable paths
# containing backslashes, quotes or control characters even when quoted.
stub=$scratch/bin/exec-stub
if [[ $stub == *[\\\"\']* || $stub == *[[:cntrl:]]* ]]; then
    echo "error: the scratch directory's path has a backslash, a quote or a control character;" \
        "set TMPDIR to a plainer directory: $scratch" >&2
    exit 1
fi
mkdir "$scratch/bin" "$scratch/preflight" "$scratch/units"
mkdir -m 0700 "$scratch/runtime"
printf '#!/bin/sh\nexit 0\n' >"$stub"
chmod +x "$stub"
quoted=${stub//%/%%}

analyze() {
    local scope=$1 file=$2
    if [[ $scope == user ]]; then
        XDG_RUNTIME_DIR="$scratch/runtime" systemd-analyze --user verify --recursive-errors=no "$file"
    else
        systemd-analyze verify --recursive-errors=no "$file"
    fi
}

# --recursive-errors=no makes warnings in the supplied unit fatal while excluding diagnostics
# from dependencies. verify runs a test manager. For example, system scope on unprivileged
# systemd 257 may fail to create /run/systemd even for a minimal fixture.
preflight() {
    local scope=$1 unit_file=$scratch/preflight/numbers-$1-preflight.service status=0 output
    printf '[Service]\nType=oneshot\nExecStart="%s"\n' "$quoted" >"$unit_file" || exit 1
    output=$(analyze "$scope" "$unit_file" 2>&1) || status=$?
    if [[ $status != 0 || -n $output ]]; then
        unavailable "systemd-analyze verify in $scope scope failed its minimal-unit preflight (exit status $status): ${output:-no output}"
        return 1
    fi
}

# Keep the unit name and line numbers. Replace the installed executable but retain its arguments.
copy_unit() {
    local unit=$1 copy=$2 installed=$3 line count=0
    while IFS= read -r line || [[ -n $line ]]; do
        if [[ $line == "ExecStart=$installed "* ]]; then
            line="ExecStart=\"$quoted\" ${line#"ExecStart=$installed "}"
            count=$((count + 1))
        fi
        printf '%s\n' "$line"
    done <"$unit" >"$copy"
    if [[ $count != 1 ]]; then
        echo "error: $unit needs exactly one line starting with ExecStart=$installed" >&2
        exit 1
    fi
}

verify() {
    local scope=$1 copy=$2 description=$3 file=$4 status=0 output
    output=$(analyze "$scope" "$copy" 2>&1) || status=$?
    if [[ $status != 0 || -n $output ]]; then
        if [[ -n $output ]]; then
            printf '%s\n' "$output" >&2
        fi
        echo "error: systemd-analyze verify of $description failed on $file (exit status $status;" \
            "the scratch copies keep that file's line numbers)" >&2
        failed=1
    else
        echo "ok: $description ($file)"
    fi
}

tower_copy=$scratch/units/numbers-tower.service
station_copy=$scratch/units/station.service
copy_unit "$tower" "$tower_copy" /usr/local/bin/tower
copy_unit "$station" "$station_copy" '%h/.local/bin/station'

if preflight system; then
    verify system "$tower_copy" "the tower unit alone" "$tower"
    # verify loads drop-ins from <unit>.d/*.conf next to the unit file.
    mkdir "$tower_copy.d"
    cp "$dropin" "$tower_copy.d/tls-files.conf"
    verify system "$tower_copy" "the tower unit with the drop-in" "$dropin"
fi

if preflight user; then
    verify user "$station_copy" "the station user unit" "$station"
fi

echo "systemd-analyze version $version"
exit "$failed"

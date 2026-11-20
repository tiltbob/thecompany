#!/usr/bin/env bash
# Run inside a Linux network namespace with only loopback up. See README.md.
set +x
set -euo pipefail
umask 077

fail() {
    printf 'Walkthrough failed: %s\n' "$*" >&2
    exit 1
}

links=$(ip -o link show up) || fail 'could not inspect network interfaces'
[[ $links == *': lo: '* && $links != *$'\n'* ]] ||
    fail 'run inside a network namespace with only the loopback interface up (see README.md)'

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
bin_dir=${WALKTHROUGH_BIN_DIR:-"$repo_dir/target/debug"}
for binary in tower station chief; do
    [[ -x "$bin_dir/$binary" ]] || fail "build $binary or set WALKTHROUGH_BIN_DIR (see README.md)"
done

running() {
    local pid=$1
    [[ $(jobs -pr) =~ (^|[[:space:]])$pid($|[[:space:]]) ]]
}

stop_job() {
    local job=$1 pid=${2:-} deadline=$((SECONDS + 10))
    if [[ -z $pid ]]; then
        pid=$(jobs -p "%$job" 2>/dev/null) || return 0
    fi
    # Signal by job name so Bash can only target a child started by this script.
    kill -TERM "%$job" 2>/dev/null || true
    while running "$pid" && (( SECONDS < deadline )); do
        sleep 0.1
    done
    if running "$pid"; then
        kill -KILL "%$job" 2>/dev/null || true
    fi
    wait "$pid" 2>/dev/null
}

cleanup() {
    local result=$? job
    trap '' HUP INT TERM
    for job in 'run_station alice' 'run_station bob' run_tower; do
        if ! stop_job "$job"; then
            printf 'Walkthrough cleanup: %s did not exit cleanly\n' "$job" >&2
            (( result != 0 )) || result=1
        fi
    done
    if [[ -n $work_dir ]]; then
        rm -rf -- "$work_dir" || result=1
    fi
    if (( result == 0 )); then
        printf 'Walkthrough passed. Bob received the message and both station sockets were removed.\n'
    fi
    exit "$result"
}

work_dir=''
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
# Keep the paths short enough for Unix sockets even when TMPDIR is long.
work_dir=$(mktemp -d /tmp/thecompany-walkthrough.XXXXXXXX) || fail 'could not create a private directory'

run_tower() {
    NO_COLOR=1 exec "$bin_dir/tower" --bind 127.0.0.1:0
}

run_station() {
    exec "$bin_dir/station" run --data-dir "$work_dir/$1" --tower "$tower_url"
}

# Keep all raw process output private. A CLI error may include a station ID.
chief_call() {
    local limit=$1
    shift
    timeout --kill-after=1s "$limit" "$bin_dir/chief" "$@" 2>"$work_dir/chief.err"
}

wait_for_station() {
    local peer=$1 pid=$2 socket=$3 deadline=$((SECONDS + 45))
    while running "$pid" && (( SECONDS < deadline )); do
        # Status uses the canonical root URL and may label its configured source.
        if [[ -S $socket ]] && chief_call 2s --station "$socket" status >"$work_dir/status" &&
            grep -Fxq -e "tower $tower_url/ connected" \
                -e "tower $tower_url/ connected (configured)" "$work_dir/status"; then
            return
        fi
        sleep 0.1
    done
    fail "$peer station did not connect to the tower"
}

tower_log="$work_dir/tower.log"
: >"$tower_log"
run_tower >"$tower_log" 2>&1 &
tower_pid=$!
tower_url=''
deadline=$((SECONDS + 15))
while running "$tower_pid" && (( SECONDS < deadline )); do
    # Using the tower's reported port avoids racing another process to bind a free port.
    tower_url=$(sed -nE 's/^.*dev tower listening url="(http:\/\/127\.0\.0\.1:[1-9][0-9]{0,4})"$/\1/p' \
        "$tower_log")
    [[ -z $tower_url ]] || break
    sleep 0.1
done
[[ -n $tower_url ]] || fail 'tower did not report a loopback listener'

run_station alice >"$work_dir/alice.log" 2>&1 &
alice_pid=$!
run_station bob >"$work_dir/bob.log" 2>&1 &
bob_pid=$!
alice_socket="$work_dir/alice/station.sock"
bob_socket="$work_dir/bob/station.sock"
wait_for_station Alice "$alice_pid" "$alice_socket"
wait_for_station Bob "$bob_pid" "$bob_socket"

alice_id=$(chief_call 10s --station "$alice_socket" id) || fail 'could not read Alice’s station ID'
bob_id=$(chief_call 10s --station "$bob_socket" id) || fail 'could not read Bob’s station ID'
chief_call 10s --station "$alice_socket" peer add bob "$bob_id" > /dev/null || fail 'Alice could not add Bob'
chief_call 10s --station "$bob_socket" peer add alice "$alice_id" > /dev/null || fail 'Bob could not add Alice'

message='hello from Alice'
chief_call 25s --station "$alice_socket" send bob "$message" > /dev/null || fail 'Alice could not deliver the message'
chief_call 10s --station "$bob_socket" log alice >"$work_dir/bob-messages" || fail 'could not read Bob’s log'
grep -Eq "^[0-9]+ [^ ]+ <- alice $message$" "$work_dir/bob-messages" || fail 'message missing from Bob’s log'

stop_job 'run_station alice' "$alice_pid" || fail 'Alice station did not shut down cleanly'
stop_job 'run_station bob' "$bob_pid" || fail 'Bob station did not shut down cleanly'
for socket in "$alice_socket" "$bob_socket"; do
    [[ ! -e $socket && ! -L $socket ]] || fail 'station socket remains after shutdown'
done
stop_job run_tower "$tower_pid" || fail 'tower did not shut down cleanly'

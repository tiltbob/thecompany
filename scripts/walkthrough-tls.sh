#!/usr/bin/env bash
# Run inside a Linux network namespace with only loopback up. See README.md.
set +x
set -euo pipefail
umask 077

fail() {
    printf 'TLS walkthrough failed: %s\n' "$*" >&2
    exit 1
}

links=$(ip -o link show up 2>/dev/null) || fail 'could not inspect network interfaces'
[[ $links == *': lo: '* && $links != *$'\n'* ]] ||
    fail 'run inside a network namespace with only the loopback interface up (see README.md)'
command -v openssl >/dev/null || fail 'OpenSSL is required'

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
    # Signal only jobs started by this shell.
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
    for job in 'run_station alice' 'run_station bob' 'run_station unlisted' run_tower; do
        if ! stop_job "$job"; then
            printf 'TLS walkthrough cleanup: %s did not exit cleanly\n' "$job" >&2
            (( result != 0 )) || result=1
        fi
    done
    if [[ -n $work_dir ]]; then
        rm -rf -- "$work_dir" || result=1
    fi
    if (( result == 0 )); then
        printf 'TLS walkthrough passed. Bob received the message, the unlisted station was denied, and all sockets were removed.\n'
    fi
    exit "$result"
}

work_dir=''
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
# Keep Unix socket paths short even when TMPDIR is long.
work_dir=$(mktemp -d /tmp/thecompany-tls-walkthrough.XXXXXXXX) || fail 'could not create a private directory'

make_certificates() {
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj '/CN=Walkthrough CA' \
        -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign' \
        -keyout "$work_dir/ca.key" -out "$work_dir/ca.pem" &&
    openssl req -new -newkey rsa:2048 -nodes -subj '/CN=127.0.0.1' \
        -keyout "$work_dir/server.key" -out "$work_dir/server.csr" &&
    openssl x509 -req -days 1 -in "$work_dir/server.csr" -CA "$work_dir/ca.pem" \
        -CAkey "$work_dir/ca.key" -CAcreateserial -extfile "$work_dir/server.ext" \
        -out "$work_dir/server.pem"
}

cat >"$work_dir/server.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=IP:127.0.0.1
EOF
make_certificates >"$work_dir/openssl.log" 2>&1 || fail 'could not generate the TLS certificates'

# The stations need the HTTPS port before the tower starts. This port is local to the namespace.
https_port=3443
tower_url="https://127.0.0.1:$https_port"
cat >"$work_dir/tower.toml" <<EOF
hostname = "127.0.0.1"
http_bind = "127.0.0.1:0"
https_bind = "127.0.0.1:$https_port"
qad = false
allow_file = "allow.txt"

[tls]
cert = "server.pem"
key = "server.key"
EOF

run_tower() {
    exec "$bin_dir/tower" serve --config "$work_dir/tower.toml"
}

run_station() {
    exec "$bin_dir/station" run --data-dir "$work_dir/$1" --tower "$tower_url" --tower-ca "$work_dir/ca.pem"
}

chief_call() {
    local limit=$1
    shift
    timeout --kill-after=1s "$limit" "$bin_dir/chief" "$@" 2>"$work_dir/chief.err"
}

wait_for_id() {
    local pid=$1 socket=$2 deadline=$((SECONDS + 15)) id
    while running "$pid" && (( SECONDS < deadline )); do
        if [[ -S $socket ]] && id=$(chief_call 2s --station "$socket" id) && [[ -n $id ]]; then
            printf '%s\n' "$id"
            return
        fi
        sleep 0.1
    done
    return 1
}

wait_for_status() {
    local pid=$1 socket=$2 expected=$3 deadline=$((SECONDS + 45))
    while running "$pid" && running "$tower_pid" && (( SECONDS < deadline )); do
        if [[ -S $socket ]] && chief_call 2s --station "$socket" status >"$work_dir/status" &&
            grep -Fxq -- "tower $tower_url/ $expected" "$work_dir/status"; then
            return
        fi
        sleep 0.1
    done
    return 1
}

run_station alice >"$work_dir/alice.log" 2>&1 &
alice_pid=$!
run_station bob >"$work_dir/bob.log" 2>&1 &
bob_pid=$!
alice_socket="$work_dir/alice/station.sock"
bob_socket="$work_dir/bob/station.sock"
unlisted_socket="$work_dir/unlisted/station.sock"

# IPC is available before the stations connect. The allow file is read once at tower startup.
alice_id=$(wait_for_id "$alice_pid" "$alice_socket") || fail 'could not read Alice’s station ID'
bob_id=$(wait_for_id "$bob_pid" "$bob_socket") || fail 'could not read Bob’s station ID'
printf '%s\n' "$alice_id" "$bob_id" >"$work_dir/allow.txt"
run_tower >"$work_dir/tower.log" 2>&1 &
tower_pid=$!

wait_for_status "$alice_pid" "$alice_socket" 'connected (configured)' || fail 'Alice did not connect to the TLS tower'
wait_for_status "$bob_pid" "$bob_socket" 'connected (configured)' || fail 'Bob did not connect to the TLS tower'
chief_call 10s --station "$alice_socket" peer add bob "$bob_id" > /dev/null || fail 'Alice could not add Bob'
chief_call 10s --station "$bob_socket" peer add alice "$alice_id" > /dev/null || fail 'Bob could not add Alice'

message='hello from Alice over TLS'
chief_call 25s --station "$alice_socket" send bob "$message" > /dev/null || fail 'Alice could not deliver the message'
chief_call 10s --station "$bob_socket" log alice >"$work_dir/bob-messages" || fail 'could not read Bob’s log'
grep -Eq "^[0-9]+ [^ ]+ <- alice $message$" "$work_dir/bob-messages" || fail 'message missing from Bob’s log'

run_station unlisted >"$work_dir/unlisted.log" 2>&1 &
unlisted_pid=$!
wait_for_status "$unlisted_pid" "$unlisted_socket" "denied (configured): not on this tower's list" ||
    fail 'the unlisted station was not denied by the TLS tower'

stop_job 'run_station alice' "$alice_pid" || fail 'Alice station did not shut down cleanly'
stop_job 'run_station bob' "$bob_pid" || fail 'Bob station did not shut down cleanly'
stop_job 'run_station unlisted' "$unlisted_pid" || fail 'the unlisted station did not shut down cleanly'
for socket in "$alice_socket" "$bob_socket" "$unlisted_socket"; do
    [[ ! -e $socket && ! -L $socket ]] || fail 'station socket remains after shutdown'
done
stop_job run_tower "$tower_pid" || fail 'tower did not shut down cleanly'

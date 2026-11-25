#!/usr/bin/env bash
# cluster_lib.sh: helpers shared by the tests that start a real cluster (sourced).
#
# Before sourcing, the test may set NP (default 3) and TEST_NAME (used for the
# temporary directories). The test exports MB_REPLICA_TIMEOUT_MS itself if it
# needs a value other than the broker's default. See test_cluster.sh for the
# environment variables (BASE_PORT, MPIRUN, MPIRUN_ARGS, BROKER_HOSTS, DATA_DIR,
# KEEP_DATA).

TEST_NAME=${TEST_NAME:-mb_cluster}
NP=${NP:-3}
# port_free <port>: nothing listens there on this host.
port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
if [ -z "${BASE_PORT:-}" ]; then
    # Below the Linux ephemeral range (32768+), which MPI's own TCP links use.
    for _ in $(seq 50); do
        BASE_PORT=$((20000 + RANDOM % 12000))
        free=1
        for ((r = 0; r < NP; r++)); do port_free $((BASE_PORT + r)) || free=0; done
        [ "$free" = 1 ] && break
    done
fi
MPIRUN=${MPIRUN:-mpirun}
MPIRUN_ARGS=${MPIRUN_ARGS:-}
MPIRUN_ENV=${MPIRUN_ENV:--x}
BROKERD=./bin/brokerd
CLI=./bin/mbcli
DUMP=./bin/mb_logdump
export MB_REPLICA_TIMEOUT_MS=${MB_REPLICA_TIMEOUT_MS:-3000}

# Open MPI's blocking receive spins inside its progress engine; yielding keeps
# several brokers on one machine from starving each other.
export OMPI_MCA_mpi_yield_when_idle=${OMPI_MCA_mpi_yield_when_idle:-1}

for bin in "$BROKERD" "$CLI"; do
    if [ ! -x "$bin" ]; then
        echo "missing $bin: run make first" >&2
        exit 2
    fi
done
if ! command -v "$MPIRUN" >/dev/null 2>&1; then
    echo "missing $MPIRUN: load an MPI module first" >&2
    exit 2
fi

read -r -a HOSTS <<< "${BROKER_HOSTS:-}"
host_of() {
    if [ "${#HOSTS[@]}" -gt "$1" ]; then echo "${HOSTS[$1]}"; else echo 127.0.0.1; fi
}

OWN_DATA=0
if [ -z "${DATA_DIR:-}" ]; then
    DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${TEST_NAME}.XXXXXX")
    OWN_DATA=1
else
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
fi
DATA_DIR=$(cd "$DATA_DIR" && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/${TEST_NAME}_work.XXXXXX")
BROKER_LOG="$DATA_DIR/brokers.log"
MPI_PID=""

CHECKS=0
FAILURES=0

cleanup() {
    if [ -n "$MPI_PID" ] && kill -0 "$MPI_PID" 2>/dev/null; then
        kill -INT "$MPI_PID" 2>/dev/null
        sleep 2
        kill -KILL "$MPI_PID" 2>/dev/null
        wait "$MPI_PID" 2>/dev/null
    fi
    if [ "${KEEP_DATA:-0}" = 1 ]; then
        echo "data kept in $DATA_DIR, client outputs in $WORK"
        return
    fi
    rm -rf "$WORK"
    if [ "$OWN_DATA" = 1 ]; then
        rm -rf "$DATA_DIR"
    fi
}
trap cleanup EXIT

check() {
    local description=$1
    shift
    CHECKS=$((CHECKS + 1))
    if "$@"; then
        echo "  ✔ $description"
    else
        FAILURES=$((FAILURES + 1))
        echo "  ✘ FALLA $description"
    fi
}

section() {
    echo ""
    echo "── $1 · $2"
}

# cli <rank> <command...>: one mbcli call against broker <rank>, never longer than 20 s.
cli() {
    local rank=$1
    shift
    timeout 20 "$CLI" "$(host_of "$rank")" $((BASE_PORT + rank)) "$@"
}

# produce <rank> <topic> <acks> <key|-> < lines: prints "partition=P offset=O" per message.
produce() {
    local rank=$1 topic=$2 acks=$3 key=$4
    if [ "$key" = "-" ]; then
        timeout 60 "$CLI" "$(host_of "$rank")" $((BASE_PORT + rank)) PRODUCE "$topic" "$acks"
    else
        timeout 60 "$CLI" "$(host_of "$rank")" $((BASE_PORT + rank)) PRODUCE "$topic" "$acks" "$key"
    fi
}

# consume <rank> <topic> <group> <seconds>: everything a fresh member receives in that time.
consume() {
    local rank=$1 topic=$2 group=$3 seconds=$4
    timeout "$seconds" "$CLI" "$(host_of "$rank")" $((BASE_PORT + rank)) CONSUME "$topic" "$group" FROM_BEGINNING 2>/dev/null
    return 0
}

# Same FNV-1a as the broker: partition of a key and coordinator of a group.
fnv1a() {
    local s=$1 h=2166136261 i c
    for ((i = 0; i < ${#s}; i++)); do
        printf -v c '%d' "'${s:i:1}"
        h=$(( ((h ^ c) * 16777619) & 0xFFFFFFFF ))
    done
    echo "$h"
}

# key_for <partitions> <partition>: a key that lands on that partition.
key_for() {
    local n=0
    while :; do
        if [ $(( $(fnv1a "k$n") % $1 )) -eq "$2" ]; then echo "k$n"; return; fi
        n=$((n + 1))
    done
}

# group_avoiding <rank> <prefix>: a group whose coordinator is not <rank>.
group_avoiding() {
    local n=0
    while :; do
        if [ $(( $(fnv1a "$2$n") % NP )) -ne "$1" ]; then echo "$2$n"; return; fi
        n=$((n + 1))
    done
}

# field <rank> <topic> <partition> <name>: one value of "topic describe".
field() {
    cli "$1" TOPIC DESCRIBE "$2" 2>/dev/null \
        | awk -v p="$3" -v f="$4" '$1 == "partition" && $2 == p {
              for (i = 3; i <= NF; i++) { split($i, kv, "="); if (kv[1] == f) print kv[2] } }'
}

# wait_until <seconds> <command...>: retries a check every 100 ms.
wait_until() {
    local deadline=$((SECONDS + $1))
    shift
    while [ "$SECONDS" -lt "$deadline" ]; do
        if "$@"; then return 0; fi
        sleep 0.1
    done
    "$@"
}

all_answer() {
    local r
    for ((r = 0; r < NP; r++)); do
        cli "$r" TOPIC LIST >/dev/null 2>&1 || return 1
    done
}

start_cluster() {
    # shellcheck disable=SC2086
    "$MPIRUN" $MPIRUN_ARGS $MPIRUN_ENV MB_REPLICA_TIMEOUT_MS -np "$NP" \
        "$BROKERD" --base-port "$BASE_PORT" --data "$DATA_DIR" >> "$BROKER_LOG" 2>&1 &
    MPI_PID=$!
    wait_until 30 all_answer
}

# stop_cluster <rank>: SHUTDOWN through that broker; the launcher must end cleanly.
STOP_RC=-1
stop_cluster() {
    local before
    before=$(grep -c ' stopped ' "$BROKER_LOG" 2>/dev/null)
    cli "$1" SHUTDOWN > "$WORK/shutdown.out" 2>&1
    local deadline=$((SECONDS + 30))
    while kill -0 "$MPI_PID" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do sleep 0.2; done
    if kill -0 "$MPI_PID" 2>/dev/null; then
        STOP_RC=124
        return 1
    fi
    wait "$MPI_PID"
    STOP_RC=$?
    MPI_PID=""
    STOPPED=$(( $(grep -c ' stopped ' "$BROKER_LOG") - before ))
    return 0
}

# offsets_contiguous <file> <partition> <first> <count>: the confirmed offsets of
# that partition are exactly first..first+count-1, in order.
offsets_contiguous() {
    local got expected
    got=$(grep "^partition=$2 " "$1" | sed 's/.*offset=\([0-9]*\).*/\1/' | tr '\n' ' ')
    expected=$(seq "$3" $(( $3 + $4 - 1 )) | tr '\n' ' ')
    [ "$got" = "$expected" ]
}

# summary <name>: final line and exit code of the test.
summary() {
    if [ "$FAILURES" -ne 0 ]; then
        echo ""
        echo "broker log ($BROKER_LOG):"
        tail -n 40 "$BROKER_LOG"
    fi
    echo ""
    if [ "$FAILURES" -eq 0 ]; then
        echo "✔ $1: $CHECKS de $CHECKS checks pasaron — todo verde"
        exit 0
    fi
    echo "✘ $1: $((CHECKS - FAILURES)) de $CHECKS checks pasaron — $FAILURES FALLO(S)"
    exit 1
}


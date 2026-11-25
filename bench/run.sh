#!/usr/bin/env bash
#
# runner: bench/run.sh [all | brokers | partitions | hot | consumers | replication ...]
#
# Metrics battery of the message broker. Scenarios:
#   brokers      throughput while the cluster grows: N brokers and N partitions
#                (BENCH_BROKERS, default "1 2 3 4"), R=1, acks=1, the producers
#                split evenly over the brokers
#   partitions   3 brokers and P partitions (BENCH_PARTITIONS, default "1 2 3 6")
#   hot          3 brokers, 3 partitions, a percentage of the messages with the
#                same key (BENCH_HOT, default "0 25 50 75 100")
#   consumers    3 brokers, BENCH_GROUP_PARTITIONS partitions (default 4) already
#                filled; a group of C members reads everything (BENCH_CONSUMERS,
#                default "1 2 3 4 6": includes more members than partitions)
#   replication  3 brokers, one partition, acks 0/1/ALL x R 1..3; the producers
#                talk to the leader, so the measure is the replication itself
#
# Every point repeats BENCH_REPS times (default 5) and bench/analyze.py takes the
# median of each one. The raw runs go to bench/results/bench_<env>_<date>.csv with
# a header of comments that describes the hardware, the MPI and the placement.
#
# Environment variables:
#   BENCH_ENV          label of the environment (default: the hostname)
#   BENCH_REPS         repetitions per point (default 5)
#   BENCH_PRODUCERS    producer threads in total (default 8)
#   BENCH_MESSAGES     messages per producer thread (default 2000)
#   BENCH_BYTES        bytes per message (default 256)
#   BENCH_PREFILL      messages written before the consumers scenario (default 40000)
#   BENCH_REPL_PRODUCERS producer threads of the replication scenario (default 4)
#   BROKER_HOSTS       IPv4 of each rank, space separated (default 127.0.0.1)
#   MPIRUN / MPIRUN_ARGS  launcher and its arguments (e.g. "--map-by node")
#   BASE_PORT          first TCP port (default 21000)
#   DATA_DIR           broker data (default a temp dir; use node-local storage)
#   BENCH_OUT          output CSV (default under bench/results/)
set -u

cd "$(dirname "$0")/.." || exit 1
ROOT=$(pwd)

REPS=${BENCH_REPS:-5}
PRODUCERS=${BENCH_PRODUCERS:-8}
MESSAGES=${BENCH_MESSAGES:-2000}
BYTES=${BENCH_BYTES:-256}
PREFILL=${BENCH_PREFILL:-40000}
REPL_PRODUCERS=${BENCH_REPL_PRODUCERS:-4}
BROKERS_LIST=${BENCH_BROKERS:-1 2 3 4}
PARTITIONS_LIST=${BENCH_PARTITIONS:-1 2 3 6}
HOT_LIST=${BENCH_HOT:-0 25 50 75 100}
CONSUMERS_LIST=${BENCH_CONSUMERS:-1 2 3 4 6}
GROUP_PARTITIONS=${BENCH_GROUP_PARTITIONS:-4}
ENV_LABEL=${BENCH_ENV:-$(hostname -s)}
MPIRUN=${MPIRUN:-mpirun}
MPIRUN_ARGS=${MPIRUN_ARGS:-}
BASE_PORT=${BASE_PORT:-21000}
export OMPI_MCA_mpi_yield_when_idle=${OMPI_MCA_mpi_yield_when_idle:-1}

SCENARIOS=("$@")
if [ "${#SCENARIOS[@]}" -eq 0 ] || [ "${SCENARIOS[0]}" = all ]; then
    SCENARIOS=(brokers partitions hot consumers replication)
fi

read -r -a HOSTS <<< "${BROKER_HOSTS:-}"
host_of() {
    if [ "${#HOSTS[@]}" -gt "$1" ]; then echo "${HOSTS[$1]}"; else echo 127.0.0.1; fi
}
# Brokers available: one per listed host, or as many as asked on one machine.
MAX_BROKERS=${#HOSTS[@]}

for bin in brokerd mbcli mbbench; do
    if [ ! -x "$ROOT/bin/$bin" ]; then
        echo "missing bin/$bin: run make first" >&2
        exit 2
    fi
done

OWN_DATA=0
if [ -z "${DATA_DIR:-}" ]; then
    DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/mb_bench.XXXXXX")
    OWN_DATA=1
fi
mkdir -p "$DATA_DIR" "$ROOT/bench/results"
STAMP=$(date +%Y%m%d_%H%M%S)
OUT=${BENCH_OUT:-$ROOT/bench/results/bench_${ENV_LABEL}_${STAMP}.csv}
LOG="$DATA_DIR/brokers.log"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/mb_bench_work.XXXXXX")
MPI_PID=""
NP=0
FAILURES=0

cli() {
    local rank=$1
    shift
    timeout 30 "$ROOT/bin/mbcli" "$(host_of "$rank")" $((BASE_PORT + rank)) "$@"
}

all_answer() {
    local r
    for ((r = 0; r < NP; r++)); do cli "$r" TOPIC LIST >/dev/null 2>&1 || return 1; done
}

stop_cluster() {
    [ -n "$MPI_PID" ] || return 0
    cli 0 SHUTDOWN >/dev/null 2>&1
    local deadline=$((SECONDS + 30))
    while kill -0 "$MPI_PID" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do sleep 0.2; done
    if kill -0 "$MPI_PID" 2>/dev/null; then
        kill -INT "$MPI_PID" 2>/dev/null
        sleep 2
    fi
    wait "$MPI_PID" 2>/dev/null
    MPI_PID=""
}

# start_cluster <brokers>: a fresh cluster with empty data.
start_cluster() {
    stop_cluster
    NP=$1
    rm -rf "${DATA_DIR:?}"/broker*
    # shellcheck disable=SC2086
    "$MPIRUN" $MPIRUN_ARGS -np "$NP" "$ROOT/bin/brokerd" --base-port "$BASE_PORT" --data "$DATA_DIR" >> "$LOG" 2>&1 &
    MPI_PID=$!
    for _ in $(seq 300); do all_answer && return 0; sleep 0.1; done
    echo "the cluster of $NP brokers did not start:" >&2
    tail -n 20 "$LOG" >&2
    return 1
}

cleanup() {
    stop_cluster
    rm -rf "$WORK"
    if [ "$OWN_DATA" = 1 ]; then rm -rf "$DATA_DIR"; fi
}
trap cleanup EXIT

# create_topic <name> <partitions> <replication>: waits until broker 0 knows the leaders.
create_topic() {
    cli 0 TOPIC CREATE "$1" "$2" "$3" >/dev/null 2>&1 || return 1
    local r
    for _ in $(seq 50); do
        local ready=1
        for ((r = 0; r < NP; r++)); do
            cli "$r" TOPIC DESCRIBE "$1" 2>/dev/null | grep -q 'leader=' || ready=0
        done
        [ "$ready" = 1 ] && return 0
        sleep 0.1
    done
    return 1
}

leader_of() { # leader_of <topic> <partition>
    cli 0 TOPIC DESCRIBE "$1" 2>/dev/null | awk -v p="$2" '$1 == "partition" && $2 == p {sub("leader=", "", $3); print $3}'
}

# load <topic> <acks> <hot> <producers> <consumers> <group> <ranks...>
# Runs one mbbench per listed broker at the same time, the producer and consumer
# threads split as evenly as possible, and prints one aggregated mbbench line:
# messages are added up over the longest of the runs, latencies are the worst.
load() {
    local topic=$1 acks=$2 hot=$3 producers=$4 consumers=$5 group=$6
    shift 6
    local ranks=("$@") n=$# i pids=()
    for ((i = 0; i < n; i++)); do
        local p=$(( producers / n + (i < producers % n ? 1 : 0) ))
        local c=$(( consumers / n + (i < consumers % n ? 1 : 0) ))
        [ $((p + c)) -eq 0 ] && continue
        timeout 900 "$ROOT/bin/mbbench" "$(host_of "${ranks[$i]}")" $((BASE_PORT + ranks[i])) "$topic" \
            "$p" "$MESSAGES" "$BYTES" "$acks" "$c" "$group" "$hot" > "$WORK/load.$i" 2>/dev/null &
        pids+=($!)
    done
    local rc=0 pid
    for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
    [ "$rc" -eq 0 ] || return 1
    cat "$WORK"/load.* | awk -F, -v producers="$producers" -v consumers="$consumers" -v acks="$acks" \
        -v messages="$MESSAGES" -v bytes="$BYTES" '
        NF >= 13 {
            produced += $6; if ($7 > seconds) seconds = $7
            if ($10 > p50) p50 = $10; if ($11 > p99) p99 = $11
            consumed += $12; if ($13 > 0) { csec = $12 / $13; if (csec > consume_seconds) consume_seconds = csec }
            lines++
        }
        END {
            if (lines == 0) exit 1
            msg_s = seconds > 0 ? produced / seconds : 0
            mb_s = msg_s * bytes / 1048576
            consume_msg_s = consume_seconds > 0 ? consumed / consume_seconds : 0
            printf "%d,%d,%d,%d,%s,%d,%.3f,%.1f,%.3f,%.3f,%.3f,%d,%.1f\n", producers, consumers, messages,
                   bytes, acks, produced, seconds, msg_s, mb_s, p50, p99, consumed, consume_msg_s
        }'
    rm -f "$WORK"/load.*
}

# record <scenario> <param> <brokers> <partitions> <replication> <acks> <hot> <rep> <mbbench line>
record() {
    echo "$ENV_LABEL,$NODES,$1,$2,$3,$4,$5,$6,$7,$8,$9" >> "$OUT"
    local line=$9
    printf "  %-11s %-8s rep=%s  %10s msg/s  consume %10s msg/s  p50 %7s ms  p99 %7s ms\n" "$1" "$2" "$8" \
        "$(echo "$line" | cut -d, -f8)" "$(echo "$line" | cut -d, -f13)" \
        "$(echo "$line" | cut -d, -f10)" "$(echo "$line" | cut -d, -f11)"
}

failed() {
    echo "  ✘ $1" >&2
    FAILURES=$((FAILURES + 1))
}

all_ranks() { seq 0 $((NP - 1)); }

# --------------------------------------------------------------------------
scenario_brokers() {
    local b rep
    for b in $BROKERS_LIST; do
        if [ "$MAX_BROKERS" -gt 0 ] && [ "$b" -gt "$MAX_BROKERS" ]; then continue; fi
        start_cluster "$b" || { failed "cluster of $b brokers"; continue; }
        for rep in $(seq 1 "$REPS"); do
            local topic="brokers_${b}_${rep}"
            create_topic "$topic" "$b" 1 || { failed "create $topic"; continue; }
            local line
            # shellcheck disable=SC2046
            line=$(load "$topic" 1 0 "$PRODUCERS" 0 none $(all_ranks)) || { failed "brokers=$b rep=$rep"; continue; }
            record brokers "$b" "$b" "$b" 1 1 0 "$rep" "$line"
        done
    done
}

scenario_partitions() {
    local p rep
    start_cluster 3 || { failed "cluster of 3 brokers"; return; }
    for p in $PARTITIONS_LIST; do
        for rep in $(seq 1 "$REPS"); do
            local topic="partitions_${p}_${rep}"
            create_topic "$topic" "$p" 1 || { failed "create $topic"; continue; }
            local line
            # shellcheck disable=SC2046
            line=$(load "$topic" 1 0 "$PRODUCERS" 0 none $(all_ranks)) || { failed "partitions=$p rep=$rep"; continue; }
            record partitions "$p" 3 "$p" 1 1 0 "$rep" "$line"
        done
    done
}

scenario_hot() {
    local h rep
    start_cluster 3 || { failed "cluster of 3 brokers"; return; }
    for h in $HOT_LIST; do
        for rep in $(seq 1 "$REPS"); do
            local topic="hot_${h}_${rep}"
            create_topic "$topic" 3 1 || { failed "create $topic"; continue; }
            local line
            # shellcheck disable=SC2046
            line=$(load "$topic" 1 "$h" "$PRODUCERS" 0 none $(all_ranks)) || { failed "hot=$h rep=$rep"; continue; }
            record hot "$h" 3 3 1 1 "$h" "$rep" "$line"
        done
    done
}

scenario_consumers() {
    local c rep
    start_cluster 3 || { failed "cluster of 3 brokers"; return; }
    local topic="consumers_topic"
    create_topic "$topic" "$GROUP_PARTITIONS" 1 || { failed "create $topic"; return; }
    # Fill the topic once; every run reads all of it with a new group.
    local saved=$MESSAGES
    MESSAGES=$(( PREFILL / PRODUCERS ))
    # shellcheck disable=SC2046
    load "$topic" 1 0 "$PRODUCERS" 0 none $(all_ranks) > /dev/null || { failed "prefill"; MESSAGES=$saved; return; }
    MESSAGES=$saved
    for c in $CONSUMERS_LIST; do
        for rep in $(seq 1 "$REPS"); do
            local line
            # shellcheck disable=SC2046
            line=$(load "$topic" 1 0 0 "$c" "group_${c}_${rep}" $(all_ranks)) || { failed "consumers=$c rep=$rep"; continue; }
            record consumers "$c" 3 "$GROUP_PARTITIONS" 1 1 0 "$rep" "$line"
        done
    done
}

scenario_replication() {
    local r acks rep
    start_cluster 3 || { failed "cluster of 3 brokers"; return; }
    for r in 1 2 3; do
        for acks in 0 1 ALL; do
            for rep in $(seq 1 "$REPS"); do
                local topic="repl_r${r}_a${acks}_${rep}"
                create_topic "$topic" 1 "$r" || { failed "create $topic"; continue; }
                local leader line
                leader=$(leader_of "$topic" 0)
                line=$(load "$topic" "$acks" 0 "$REPL_PRODUCERS" 0 none "$leader") || { failed "R=$r acks=$acks rep=$rep"; continue; }
                record replication "R${r}-${acks}" 3 1 "$r" "$acks" 0 "$rep" "$line"
            done
        done
    done
}

# --------------------------------------------------------------------------
NODES=$(echo "${BROKER_HOSTS:-127.0.0.1}" | tr ' ' '\n' | grep . | sort -u | wc -l)
{
    echo "# message broker benchmark"
    echo "# date: $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "# env: $ENV_LABEL | batch host: $(hostname) | kernel: $(uname -srm)"
    if [ -r /etc/os-release ]; then
        echo "# distro: $(. /etc/os-release; echo "${PRETTY_NAME:-unknown}")"
    fi
    echo "# cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | sed 's/^ //') x $(nproc)"
    echo "# mpi: $("$MPIRUN" --version 2>&1 | head -n 1)"
    echo "# hosts: ${BROKER_HOSTS:-127.0.0.1 (all local)}"
    echo "# load: producers=$PRODUCERS messages_per_producer=$MESSAGES bytes=$BYTES reps=$REPS"
    echo "env,nodes,scenario,param,brokers,partitions,replication,acks,hot,rep,producers,consumers,messages,bytes,acks_mbbench,produced,seconds,msg_s,mb_s,latency_median_ms,latency_p99_ms,consumed,consume_msg_s"
} > "$OUT"

echo "═══ message broker benchmark · ${SCENARIOS[*]} · $NODES node(s)"
echo "    output: $OUT"
for scenario in "${SCENARIOS[@]}"; do
    case "$scenario" in
        brokers|partitions|hot|consumers|replication)
            echo ""
            echo "── $scenario"
            "scenario_$scenario"
            ;;
        *)
            echo "unknown scenario '$scenario'" >&2
            FAILURES=$((FAILURES + 1))
            ;;
    esac
done
stop_cluster

echo ""
echo "✔ results in $OUT ($FAILURES failures)"
if command -v python3 >/dev/null 2>&1; then
    python3 "$ROOT/bench/analyze.py" "$OUT"
fi
[ "$FAILURES" -eq 0 ]

#!/usr/bin/env bash
# kabre.sh: drives Kabré from your machine. One SSH connection is opened and
# reused (ControlMaster), so the password is asked once per run.
#
#   scripts/kabre/kabre.sh sync               copy the repository to Kabré (rsync)
#   scripts/kabre/kabre.sh setup              sync + check modules, MPI and build
#   scripts/kabre/kabre.sh tests              sync + make test on one node, wait, show the log
#   scripts/kabre/kabre.sh multinode          sync + tests with one broker per node (3 nodes)
#   scripts/kabre/kabre.sh bench [sbatch...]  sync + benchmark battery, wait, pull results, analyze
#   scripts/kabre/kabre.sh all                tests, multinode and bench, one after the other
#   scripts/kabre/kabre.sh pull               download bench/results/*.csv and the job logs
#   scripts/kabre/kabre.sh status             your jobs in the queue
#
# Extra arguments after tests/multinode/bench go to sbatch, e.g.
#   scripts/kabre/kabre.sh bench --nodes=1 --ntasks-per-node=4
#   scripts/kabre/kabre.sh multinode -p <partition>
#
# Environment:
#   KABRE_HOST  ssh destination (default "kabre": the Host entry of ~/.ssh/config
#               with HostName kabre.cenat.ac.cr, Port 22022 and your user)
#   KABRE_DIR   repository directory on Kabré, relative to its home
#               (default message_broker_distribuido)
#   KABRE_POLL  seconds between queue checks while waiting (default 20)
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
HOST=${KABRE_HOST:-kabre}
RDIR=${KABRE_DIR:-message_broker_distribuido}
POLL=${KABRE_POLL:-20}
LOGS="$REPO_ROOT/bench/results/kabre_logs"

if [ "$HOST" = kabre ] && ! ssh -G kabre 2>/dev/null | grep -qi '^hostname kabre.cenat'; then
    echo "No 'kabre' entry in ~/.ssh/config. Add one (see scripts/kabre/README.md) or set" >&2
    echo "KABRE_HOST=<user>@kabre.cenat.ac.cr and KABRE_SSH_PORT=22022." >&2
    exit 1
fi

CONTROL="$HOME/.ssh/mb-kabre-%r@%h:%p"
SSH_OPTS=(-o ControlMaster=auto -o "ControlPath=$CONTROL" -o ControlPersist=15m)
if [ -n "${KABRE_SSH_PORT:-}" ]; then
    SSH_OPTS+=(-p "$KABRE_SSH_PORT")
fi

rssh() { ssh "${SSH_OPTS[@]}" "$HOST" "$@"; }

do_sync() {
    echo "[sync] $REPO_ROOT -> $HOST:~/$RDIR"
    rsync -az --delete -e "ssh ${SSH_OPTS[*]}" \
        --exclude='.git/' --exclude='build/' --exclude='bin/' --exclude='data/' \
        --exclude='.cluster_data_*/' --exclude='bench/results/' \
        --exclude='*.out' --exclude='*.err' --exclude='*.o' \
        "$REPO_ROOT/" "$HOST:$RDIR/"
}

do_pull() {
    mkdir -p "$REPO_ROOT/bench/results" "$LOGS"
    echo "[pull] results and job logs from $HOST:~/$RDIR"
    rsync -az -e "ssh ${SSH_OPTS[*]}" --include='*.csv' --exclude='*' \
        "$HOST:$RDIR/bench/results/" "$REPO_ROOT/bench/results/" 2>/dev/null || true
    rsync -az -e "ssh ${SSH_OPTS[*]}" --include='mb_*.out' --include='mb_*.err' --exclude='*' \
        "$HOST:$RDIR/" "$LOGS/" 2>/dev/null || true
}

# submit <slurm script> [sbatch args...]: prints the job id.
submit() {
    local script=$1
    shift
    rssh "cd '$RDIR' && sbatch --parsable $* scripts/kabre/$script" | tail -n 1 | cut -d';' -f1
}

# wait_job <id>: waits until the job leaves the queue, then shows its result.
wait_job() {
    local id=$1 state
    echo "[wait] job $id (checking every ${POLL}s)"
    while :; do
        state=$(rssh "squeue -h -j $id -o %T" 2>/dev/null)
        [ -z "$state" ] && break
        printf '       %s %s\n' "$(date +%H:%M:%S)" "$state"
        sleep "$POLL"
    done
    state=$(rssh "sacct -n -X -j $id -o State,ExitCode 2>/dev/null | head -n 1" | xargs)
    echo "[done] job $id: ${state:-finished}"
    do_pull
    local out
    out=$(ls -t "$LOGS"/mb_*_"$id".out 2>/dev/null | head -n 1)
    if [ -n "$out" ]; then
        echo "------ $out (last 40 lines)"
        tail -n 40 "$out"
    fi
    case "$state" in COMPLETED*|"") return 0 ;; *) return 1 ;; esac
}

run_job() { # run_job <slurm script> [sbatch args...]
    local id
    id=$(submit "$@")
    if ! [[ "$id" =~ ^[0-9]+$ ]]; then
        echo "[✘] sbatch did not return a job id: $id" >&2
        return 1
    fi
    echo "[sbatch] $1 -> job $id"
    wait_job "$id"
}

command=${1:-}
shift || true
case "$command" in
    sync)      do_sync ;;
    setup)     do_sync && rssh "cd '$RDIR' && bash scripts/kabre/setup_env.sh" ;;
    tests)     do_sync && run_job run_tests.slurm "$@" ;;
    multinode) do_sync && run_job run_tests_multinode.slurm "$@" ;;
    bench)     do_sync && run_job run_bench.slurm "$@" && \
                   { command -v python3 >/dev/null && python3 "$REPO_ROOT/bench/analyze.py" all && python3 "$REPO_ROOT/bench/plot.py" || true; } ;;
    all)       do_sync && run_job run_tests.slurm && run_job run_tests_multinode.slurm \
                   && run_job run_bench.slurm ;;
    pull)      do_pull ;;
    status)    rssh 'squeue -u "$USER"' ;;
    *)
        sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
        exit 2
        ;;
esac

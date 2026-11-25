#!/usr/bin/env bash
#SBATCH --job-name=mb_bench
#SBATCH --output=mb_bench_%j.out
#SBATCH --error=mb_bench_%j.err
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=02:00:00
#
# Metrics battery (bench/run.sh): brokers, partitions, hot key, consumer group and
# replication cost, 5 repetitions per point.
#   sbatch scripts/kabre/run_bench.slurm                            one broker per node (up to 4)
#   sbatch --nodes=1 --ntasks-per-node=4 scripts/kabre/run_bench.slurm   every broker on one node
# Pick scenarios or change the load with --export, e.g.
#   sbatch --export=ALL,BENCH_SCENARIOS="brokers replication",BENCH_MESSAGES=5000 scripts/kabre/run_bench.slurm
# Results: bench/results/bench_<env>_<date>.csv (+ _summary.csv).

set -uo pipefail
for prologue in "${SLURM_SUBMIT_DIR:-.}/scripts/kabre/common.sh" "${SLURM_SUBMIT_DIR:-.}/common.sh" \
                "$HOME/message_broker_distribuido/scripts/kabre/common.sh"; do
    # shellcheck source=scripts/kabre/common.sh
    [ -f "$prologue" ] && { source "$prologue"; break; }
done

NODES=${SLURM_JOB_NUM_NODES:-1}
# Ranks available: one per node, or every task of a single node.
BROKERS=$(( NODES > 1 ? NODES : ${SLURM_NTASKS:-4} ))
mb_job_banner "Message broker: benchmark battery (up to $BROKERS brokers, $NODES node(s))"
mb_build

if [ "$NODES" -gt 1 ]; then
    LAYOUT_ARGS=$MB_ONE_PER_NODE
    LAYOUT=${NODES}n
else
    LAYOUT_ARGS=""
    LAYOUT=1n
fi
# shellcheck disable=SC2086
HOSTS=$(mb_rank_hosts "$BROKERS" $LAYOUT_ARGS)
echo "[map] brokers on: $HOSTS"

# Node-local storage: the shared home would measure NFS, not the broker.
DATA_DIR="${TMPDIR:-/tmp}/mb_bench_${SLURM_JOB_ID:-local}"

# shellcheck disable=SC2086
BROKER_HOSTS="$HOSTS" MPIRUN_ARGS="$LAYOUT_ARGS" DATA_DIR="$DATA_DIR" \
BENCH_ENV="${BENCH_ENV:-kabre-${LAYOUT}}" \
    bash bench/run.sh ${BENCH_SCENARIOS:-all}
rc=$?

if command -v srun >/dev/null 2>&1 && [ -n "${SLURM_JOB_ID:-}" ]; then
    srun --ntasks-per-node=1 --ntasks="$NODES" rm -rf "$DATA_DIR" 2>/dev/null
else
    rm -rf "$DATA_DIR"
fi

echo "=========================================================="
if [ "$rc" -eq 0 ]; then echo " [✔] benchmark done: bench/results/"; else echo " [✘] benchmark had failures (exit $rc)"; fi
echo "=========================================================="
exit "$rc"

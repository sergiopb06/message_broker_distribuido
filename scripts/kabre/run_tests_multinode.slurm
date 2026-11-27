#!/usr/bin/env bash
#SBATCH --job-name=mb_tests_mn
#SBATCH --output=mb_tests_mn_%j.out
#SBATCH --error=mb_tests_mn_%j.err
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=00:20:00
#
# The MPI layer, the integration test and the cluster test with one broker per
# node: replication, pause/resume and shutdown travel over the real interconnect.
# The clients run on the batch node and reach every broker by its IPv4 address.
#   sbatch scripts/kabre/run_tests_multinode.slurm
#   sbatch --nodes=4 scripts/kabre/run_tests_multinode.slurm     (4 brokers)

set -uo pipefail
for prologue in "${SLURM_SUBMIT_DIR:-.}/scripts/kabre/common.sh" "${SLURM_SUBMIT_DIR:-.}/common.sh" \
                "$HOME/message_broker_distribuido/scripts/kabre/common.sh"; do
    # shellcheck source=scripts/kabre/common.sh
    [ -f "$prologue" ] && { source "$prologue"; break; }
done

NODES=${SLURM_JOB_NUM_NODES:-3}
mb_job_banner "Message broker: tests with $NODES brokers on $NODES nodes"
mb_build

echo "[map] rank -> host with: $MPIRUN $MB_ONE_PER_NODE -np $NODES"
# shellcheck disable=SC2086
HOSTS=$(mb_rank_hosts "$NODES" $MB_ONE_PER_NODE)
echo "      $HOSTS"
if [ "$(echo "$HOSTS" | tr ' ' '\n' | grep -c . )" -ne "$NODES" ] \
   || [ "$(echo "$HOSTS" | tr ' ' '\n' | grep . | sort -u | wc -l)" -ne "$NODES" ]; then
    echo "[✘] expected one rank on each of the $NODES nodes"
    exit 1
fi

rc=0
echo ""
echo "[test] MPI layer across nodes"
# shellcheck disable=SC2086
"$MPIRUN" $MB_ONE_PER_NODE -np "$NODES" ./bin/test_mpi_layer || rc=1

echo ""
echo "[test] clients and consumer groups, one broker per node"
NP="$NODES" BROKER_HOSTS="$HOSTS" MPIRUN_ARGS="$MB_ONE_PER_NODE" \
    bash tests/test_integration.sh || rc=1

echo ""
echo "[test] cluster test, one broker per node"
# The consistency check reads every broker's log: keep the data on the shared home.
DATA_DIR="$(pwd)/.cluster_data_${SLURM_JOB_ID:-local}" \
NP="$NODES" BROKER_HOSTS="$HOSTS" MPIRUN_ARGS="$MB_ONE_PER_NODE" \
    bash tests/test_cluster.sh || rc=1
rm -rf "$(pwd)/.cluster_data_${SLURM_JOB_ID:-local}"

echo "=========================================================="
if [ "$rc" -eq 0 ]; then echo " [✔] multi-node tests passed on: $HOSTS"; else echo " [✘] multi-node tests failed"; fi
echo "=========================================================="
exit "$rc"

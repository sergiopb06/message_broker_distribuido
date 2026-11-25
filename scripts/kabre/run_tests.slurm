#!/usr/bin/env bash
#SBATCH --job-name=mb_tests
#SBATCH --output=mb_tests_%j.out
#SBATCH --error=mb_tests_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=3
#SBATCH --cpus-per-task=2
#SBATCH --time=00:20:00
#
# Whole suite on one node: unit tests, MPI layer (3 processes) and the cluster
# test with 3 brokers on the same node (make test).
#   sbatch scripts/kabre/run_tests.slurm

set -uo pipefail
for prologue in "${SLURM_SUBMIT_DIR:-.}/scripts/kabre/common.sh" "${SLURM_SUBMIT_DIR:-.}/common.sh" \
                "$HOME/message_broker_distribuido/scripts/kabre/common.sh"; do
    # shellcheck source=scripts/kabre/common.sh
    [ -f "$prologue" ] && { source "$prologue"; break; }
done

mb_job_banner "Message broker: make test on one node"
mb_build

echo "[test] make test"
make test MPIRUN="$MPIRUN"
rc=$?

echo "=========================================================="
if [ "$rc" -eq 0 ]; then echo " [✔] make test passed on ${SLURMD_NODENAME:-$(hostname)}"; else echo " [✘] make test failed (exit $rc)"; fi
echo "=========================================================="
exit "$rc"

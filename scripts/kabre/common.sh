#!/usr/bin/env bash
# common.sh: prologue shared by the Slurm jobs (sourced). Moves to the repository
# root, loads the toolchain and prints where the job runs.

if [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "$SLURM_SUBMIT_DIR/Makefile" ]; then
    cd "$SLURM_SUBMIT_DIR" || exit 1
elif [ -n "${SLURM_SUBMIT_DIR:-}" ] && [ -f "$SLURM_SUBMIT_DIR/../../Makefile" ]; then
    cd "$SLURM_SUBMIT_DIR/../.." || exit 1
elif [ ! -f Makefile ] && [ -f "$HOME/message_broker_distribuido/Makefile" ]; then
    cd "$HOME/message_broker_distribuido" || exit 1
fi

# shellcheck source=scripts/kabre/env.sh
source scripts/kabre/env.sh

# Launcher arguments that put one broker per node.
if [ "$MB_MPI_FLAVOR" = mpich ]; then
    MB_ONE_PER_NODE=${MB_ONE_PER_NODE:--ppn 1}
else
    MB_ONE_PER_NODE=${MB_ONE_PER_NODE:---map-by node}
fi

mb_job_banner() { # mb_job_banner <title>
    echo "=========================================================="
    echo " $1"
    echo " Date:      $(date)"
    echo " Job:       ${SLURM_JOB_ID:-none} (${SLURM_JOB_NUM_NODES:-1} node(s): ${SLURM_JOB_NODELIST:-$(hostname)})"
    echo " Batch host ${SLURMD_NODENAME:-$(hostname)}, $(nproc) CPUs"
    echo " Directory: $(pwd)"
    mb_print_env | sed 's/^/ /'
    echo "=========================================================="
}

mb_build() {
    echo "[build] make clean all"
    make clean >/dev/null
    if ! make -j"$(nproc)" all; then
        echo "[✘] build failed"
        exit 1
    fi
}

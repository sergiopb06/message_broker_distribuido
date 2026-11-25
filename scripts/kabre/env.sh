#!/usr/bin/env bash
# env.sh: loads the compiler and the MPI the broker needs. Sourced (not run) by
# setup_env.sh and by every Slurm job, so all of them use the same toolchain:
#
#   source scripts/kabre/env.sh
#
# Picks the newest gcc module and an MPI module (Open MPI first, then MPICH) that
# give MPI_THREAD_MULTIPLE. To force exact modules:
#   MB_MODULES="gcc/12.2.0 openmpi/4.1.5" source scripts/kabre/env.sh
#
# Exports: MPIRUN (launcher), MPIRUN_ENV (how the launcher forwards a variable),
# MB_MPI_FLAVOR (openmpi | mpich | unknown).

# Lmod/Environment Modules are shell functions that a batch job may not have yet.
if ! type module >/dev/null 2>&1; then
    for init in /etc/profile.d/modules.sh /etc/profile.d/lmod.sh /usr/share/lmod/lmod/init/bash \
                /usr/share/Modules/init/bash; do
        if [ -f "$init" ]; then
            # shellcheck disable=SC1090
            . "$init"
            break
        fi
    done
fi

mb_newest_module() { # mb_newest_module <regex>: newest available module matching it
    module -t avail 2>&1 | grep -E "$1" | grep -v ':$' | sed 's/(default)//' | sort -V | tail -n 1
}

if type module >/dev/null 2>&1; then
    if [ -n "${MB_MODULES:-}" ]; then
        # shellcheck disable=SC2086
        module load $MB_MODULES
    elif ! command -v mpicc >/dev/null 2>&1; then
        gcc_module=$(mb_newest_module '^gcc/')
        [ -n "$gcc_module" ] && module load "$gcc_module" 2>/dev/null
        mpi_module=""
        for pattern in '^(mpi/)?openmpi' '^(mpi/)?mpich'; do
            mpi_module=$(mb_newest_module "$pattern")
            [ -n "$mpi_module" ] && break
        done
        [ -n "$mpi_module" ] && module load "$mpi_module" 2>/dev/null
        python_module=$(mb_newest_module '^python/3')
        if ! command -v python3 >/dev/null 2>&1 && [ -n "$python_module" ]; then
            module load "$python_module" 2>/dev/null
        fi
        unset gcc_module mpi_module python_module
    fi
fi

export PATH="$HOME/.local/bin:$PATH"

MPIRUN=${MPIRUN:-mpirun}
if command -v ompi_info >/dev/null 2>&1; then
    MB_MPI_FLAVOR=openmpi
    MPIRUN_ENV=${MPIRUN_ENV:--x}
    # Open MPI's blocking receive spins inside its progress engine; yielding keeps
    # brokers that share a node from starving each other.
    export OMPI_MCA_mpi_yield_when_idle=${OMPI_MCA_mpi_yield_when_idle:-1}
elif command -v mpichversion >/dev/null 2>&1 || mpirun --version 2>&1 | grep -qi 'hydra\|mpich'; then
    MB_MPI_FLAVOR=mpich
    MPIRUN_ENV=${MPIRUN_ENV:--genvlist}
else
    MB_MPI_FLAVOR=unknown
    MPIRUN_ENV=${MPIRUN_ENV:--x}
fi
export MPIRUN MPIRUN_ENV MB_MPI_FLAVOR

mb_print_env() {
    echo "[i] gcc:    $(gcc --version 2>/dev/null | head -n 1)"
    echo "[i] mpicc:  $(command -v mpicc || echo 'NOT FOUND')"
    echo "[i] mpirun: $(command -v "$MPIRUN" || echo 'NOT FOUND') ($MB_MPI_FLAVOR)"
    if [ "$MB_MPI_FLAVOR" = openmpi ]; then
        echo "[i] Open MPI $(ompi_info --version 2>/dev/null | head -n 1 | grep -oE '[0-9]+(\.[0-9]+)+')," \
             "$(ompi_info 2>/dev/null | grep -i 'thread support' | sed 's/^ *//')"
    fi
    echo "[i] python: $(python3 --version 2>/dev/null || echo 'NOT FOUND')"
}

# mb_ipv4 <host>: the IPv4 address of a host (mbcli and mbbench take IPv4 literals).
mb_ipv4() {
    local ip
    ip=$(getent ahostsv4 "$1" 2>/dev/null | awk 'NR == 1 {print $1}')
    if [ -z "$ip" ] && [ "$1" = "$(hostname)" ]; then
        ip=127.0.0.1 # the batch node itself, known only by an IPv6 name
    fi
    echo "${ip:-$1}"
}

# Each broker listens on BASE_PORT + rank of the node it runs on. Prints the IPv4
# address of every rank, in rank order, for the launcher arguments given (the same
# mapping the brokers will get).
mb_rank_hosts() { # mb_rank_hosts <np> [launcher args...]
    local np=$1 host
    shift
    # shellcheck disable=SC2016
    for host in $("$MPIRUN" "$@" -np "$np" sh -c 'echo "${OMPI_COMM_WORLD_RANK:-${PMI_RANK:-${SLURM_PROCID:-0}}} $(hostname)"' \
                  | sort -n | awk '{print $2}'); do
        printf '%s ' "$(mb_ipv4 "$host")"
    done
}

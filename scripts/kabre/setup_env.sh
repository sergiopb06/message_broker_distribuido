#!/usr/bin/env bash
# setup_env.sh: one-time check of the toolchain on Kabré (run it on the login node).
#
#   bash scripts/kabre/setup_env.sh
#
# Loads the modules with env.sh, shows what was picked, checks that the MPI gives
# MPI_THREAD_MULTIPLE (the broker refuses to start otherwise) and that the project
# compiles without warnings. Nothing is installed and ~/.bashrc is not touched.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 1
# shellcheck source=scripts/kabre/env.sh
source scripts/kabre/env.sh

echo "=========================================================="
echo " Kabré environment for the message broker"
echo "=========================================================="
mb_print_env

ok=1
if ! command -v mpicc >/dev/null 2>&1; then
    echo "[✘] mpicc not found. Available MPI modules:"
    module -t avail 2>&1 | grep -iE 'mpi' | sed 's/^/      /'
    echo "    Pick one with: MB_MODULES=\"gcc/<v> openmpi/<v>\" bash scripts/kabre/setup_env.sh"
    exit 1
fi

std=$(make -s -f - <<'EOF' 2>/dev/null
include Makefile
print-std: ; @echo $(STD)
EOF
)
echo "[i] C standard the Makefile will use: -std=${std:-?}"
if [ "$std" = c11 ]; then
    echo "[!] This gcc knows neither c23 nor c2x; load a newer gcc module (MB_MODULES) for the official flags."
fi

# MPI_THREAD_MULTIPLE probe: one process, a few milliseconds.
probe_dir=$(mktemp -d)
cat > "$probe_dir/probe.c" <<'EOF'
#include <mpi.h>
#include <stdio.h>
int main(int argc, char **argv) {
    int provided = 0;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    printf("%s\n", provided >= MPI_THREAD_MULTIPLE ? "MULTIPLE" : "LOWER");
    MPI_Finalize();
    return 0;
}
EOF
if mpicc -o "$probe_dir/probe" "$probe_dir/probe.c" 2>/dev/null; then
    level=$(timeout 60 "$MPIRUN" -np 1 "$probe_dir/probe" 2>/dev/null | tail -n 1)
    if [ "$level" = MULTIPLE ]; then
        echo "[✔] MPI_THREAD_MULTIPLE available"
    elif [ -z "$level" ]; then
        echo "[!] could not run the probe here (login node?); run_tests.slurm checks it again"
    else
        echo "[✘] this MPI does not give MPI_THREAD_MULTIPLE: try another MPI module"
        ok=0
    fi
else
    echo "[✘] mpicc could not compile a trivial program"
    ok=0
fi
rm -rf "$probe_dir"

echo "[+] Building (make clean all)..."
if make clean >/dev/null && make -j"$(nproc 2>/dev/null || echo 4)" all > /tmp/mb_build_$$.log 2>&1; then
    if grep -q 'warning' /tmp/mb_build_$$.log; then
        echo "[✘] the build has warnings:"
        grep 'warning' /tmp/mb_build_$$.log | head
        ok=0
    else
        echo "[✔] build without warnings"
    fi
else
    echo "[✘] build failed:"
    tail -n 20 /tmp/mb_build_$$.log
    ok=0
fi
rm -f /tmp/mb_build_$$.log

echo "=========================================================="
if [ "$ok" = 1 ]; then
    echo "[✔] Ready. Next: sbatch scripts/kabre/run_tests.slurm"
    exit 0
fi
echo "[✘] Fix the items above before submitting jobs."
exit 1

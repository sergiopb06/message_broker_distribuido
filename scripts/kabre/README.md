# Ejecución en el clúster Kabré (CeNAT)

Scripts para compilar, probar y medir el message broker en Kabré con Slurm, en
uno o en varios nodos. Todo se maneja desde tu máquina con `kabre.sh`: sincroniza
el repositorio, envía el trabajo, espera a que termine, descarga el log y los
resultados.

## Archivos

| Archivo | Dónde corre | Qué hace |
| :--- | :--- | :--- |
| `kabre.sh` | tu máquina | Punto de entrada: `sync`, `setup`, `tests`, `multinode`, `bench`, `all`, `pull`, `status`. Abre una sola conexión SSH (ControlMaster) y la reutiliza: la contraseña se pide una vez. |
| `sync_to_kabre.sh` / `pull_results.sh` | tu máquina | Atajos de `kabre.sh sync` y `kabre.sh pull`. |
| `env.sh` | Kabré | Carga el gcc más nuevo y un MPI (Open MPI primero, luego MPICH). Lo usan todos los trabajos. `MB_MODULES` fija módulos exactos. |
| `common.sh` | Kabré | Prólogo de los trabajos: ubica la raíz del repo, carga `env.sh`, imprime el entorno y compila. |
| `setup_env.sh` | Kabré (login) | Revisión única: módulos, `MPI_THREAD_MULTIPLE` y compilación sin advertencias. No instala nada ni toca `~/.bashrc`. |
| `run_tests.slurm` | Kabré | `make test` en un nodo: pruebas unitarias, capa MPI (3 procesos) y clúster de 3 brokers. |
| `run_tests_multinode.slurm` | Kabré | Capa MPI y prueba del clúster con **un broker por nodo** (3 nodos por omisión). Los clientes corren en el nodo del lote y llegan a cada broker por su IP. |
| `run_bench.slurm` | Kabré | Batería de métricas (`bench/run.sh`): brokers, particiones, clave caliente, grupo de consumidores y costo de la replicación, 5 repeticiones. Un broker por nodo (hasta 4) o todos en un nodo. Datos en el disco local de cada nodo. |

## Configuración de SSH (una vez)

Kabré atiende SSH en el puerto **22022**. En tu `~/.ssh/config`:

```sshconfig
Host kabre
    HostName kabre.cenat.ac.cr
    User <tu_usuario>
    Port 22022
```

`kabre.sh` usa ese alias. Sin alias: `KABRE_HOST=<usuario>@kabre.cenat.ac.cr KABRE_SSH_PORT=22022`.
El repositorio queda en `~/message_broker_distribuido` (cambiable con `KABRE_DIR`).

## Flujo

```bash
# 1. Una vez: copiar el repo, revisar módulos y compilar en el nodo de login
scripts/kabre/kabre.sh setup

# 2. Pruebas en un nodo y en varios nodos (envía, espera y muestra el final del log)
scripts/kabre/kabre.sh tests
scripts/kabre/kabre.sh multinode

# 3. Métricas con un broker por nodo, y con todos los brokers en un solo nodo
scripts/kabre/kabre.sh bench
scripts/kabre/kabre.sh bench --nodes=1 --ntasks-per-node=4

# O todo seguido
scripts/kabre/kabre.sh all
```

Los argumentos después de `tests`, `multinode` o `bench` van directo a `sbatch`
(por ejemplo `-p <partición>` o `--nodes=4`). Los logs de cada trabajo quedan en
`bench/results/kabre_logs/` y los CSV del benchmark en `bench/results/`; `kabre.sh bench`
corre al final `bench/analyze.py all` y `bench/plot.py`, que comparan Kabré con los
resultados locales que haya en la misma carpeta.

Sin `kabre.sh`, dentro de Kabré: `sbatch scripts/kabre/run_tests.slurm`, `squeue -u $USER`
y `cat mb_tests_<id>.out`.

## Problemas conocidos y decisiones

1. **`MPI_THREAD_MULTIPLE`.** El broker lo exige (un hilo recibe mientras otros envían)
   y aborta con un mensaje claro si el MPI no lo da. `setup_env.sh` lo verifica; si un
   módulo no lo soporta, probar otro con `MB_MODULES="gcc/<v> openmpi/<v>"`.
2. **Estándar de C.** El `Makefile` usa `-std=c23`, o `-std=c2x`, o `-std=c11` si el gcc
   es viejo (GCC 8 de AlmaLinux 8). `env.sh` carga el gcc más nuevo disponible para
   compilar con las banderas oficiales; `setup_env.sh` avisa si quedó en `c11`.
3. **Un broker por nodo.** Con Open MPI se lanza con `--map-by node` (con MPICH,
   `-ppn 1`). Antes de arrancar, el trabajo pregunta a `mpirun` en qué nodo queda cada
   rank, para que los clientes se conecten al puerto `base + rank` del nodo correcto.
4. **Clientes con IPv4.** `mbcli` y `mbbench` aceptan una dirección IPv4, no un nombre;
   los scripts traducen el nombre de cada nodo con `getent ahostsv4`.
5. **Datos.** Las pruebas multinodo guardan los datos en el home compartido, porque la
   prueba de consistencia lee el log de cada broker desde el nodo del lote. El benchmark
   usa el `/tmp` local de cada nodo para medir el broker y no el NFS.
6. **CPU del receptor MPI.** El `MPI_Probe` bloqueante de Open MPI gira dentro de su
   motor de progreso, así que varios brokers en un nodo compiten por CPU; los scripts activan
   `OMPI_MCA_mpi_yield_when_idle=1` para que cedan el procesador.

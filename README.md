# Message broker distribuido

Intermediario de mensajes al estilo de Kafka, en C con Pthreads y MPI: tópicos con
particiones (logs ordenados y persistentes), replicación *pull* con ISR y high
watermark, grupos de consumidores con rebalanceo, pausa y reanudación de brokers y
apagado ordenado de todo el clúster.

- `brokerd`: un proceso MPI por broker. El broker `r` escucha TCP en `base + r` y
  guarda sus datos en `<data>/broker<r>/`. El broker 0 es el controlador de metadatos.
- `mbcli`: cliente de línea de comandos; se conecta a cualquier broker.
- `mbbench`: generador de carga (rendimiento y latencia, salida CSV).

## Requisitos y compilación

Linux, un compilador C23 (GCC 13+; con GCC 8–12 el `Makefile` cae a `-std=c2x` o
`-std=c11`) y un MPI con **`MPI_THREAD_MULTIPLE`** (Open MPI 4/5 o MPICH).

```bash
make            # bin/brokerd, bin/mbcli, bin/mbbench y las pruebas
make clean
```

Banderas: `mpicc -std=c23 -Wall -Wextra -pedantic -O2 -pthread`, sin advertencias.

## Ejecución en una máquina

```bash
mpirun -np 3 ./bin/brokerd --base-port 9000 --data ./data
# broker 0 listening on port 9000, broker 1 on 9001, broker 2 on 9002
```

Si la máquina tiene menos núcleos que brokers: `mpirun --oversubscribe -np 3 ...`.
Con Open MPI conviene `export OMPI_MCA_mpi_yield_when_idle=1` (ver "Decisiones").

## Ejecución en varios nodos

Un broker por nodo; los clientes se conectan a la IP de cada nodo y al puerto `base + rank`.

```bash
# Open MPI, con un archivo de hosts (un slot por nodo)
mpirun -np 3 --hostfile hosts --map-by node ./bin/brokerd --base-port 9000 --data /tmp/mb
# MPICH (Hydra)
mpiexec -n 3 -f hosts -ppn 1 ./bin/brokerd --base-port 9000 --data /tmp/mb
```

El directorio de datos puede ser local a cada nodo (cada broker usa su propio
`broker<r>/`). En Kabré todo está automatizado con Slurm: ver
[`scripts/kabre/README.md`](scripts/kabre/README.md).

## Uso del cliente

```
mbcli <host:puerto | host puerto> <comando>
  topic create <tópico> -p <particiones> -r <replicación> [--retention-bytes <bytes>]
  topic list
  topic describe <tópico>                líder, réplicas, ISR, HWM y LEO por partición
  produce <tópico> [-k <clave>] [--acks 0|1|all]   una línea de stdin = un mensaje (acks por omisión: all)
  consume <tópico> -g <grupo> [--from-beginning]
  group describe <grupo>                 dueño, offset confirmado y lag por partición
  broker pause|resume <id>
  shutdown                               apagado ordenado de todo el clúster
```

```bash
./bin/mbcli 127.0.0.1:9000 topic create pedidos -p 3 -r 2
printf "pedido 1\npedido 2\n" | ./bin/mbcli 127.0.0.1:9001 produce pedidos -k cliente42 --acks all
#   partition=1 offset=0
#   partition=1 offset=1
./bin/mbcli 127.0.0.1:9002 consume pedidos -g facturacion --from-beginning
./bin/mbcli 127.0.0.1:9000 group describe facturacion

./bin/mbcli 127.0.0.1:9000 broker pause 2
./bin/mbcli 127.0.0.1:9000 topic describe pedidos    # tras ~3 s el broker 2 sale del ISR
./bin/mbcli 127.0.0.1:9000 broker resume 2           # se pone al día y vuelve al ISR
./bin/mbcli 127.0.0.1:9000 shutdown
```

También se acepta la forma posicional del protocolo, que usan las pruebas y los
scripts: `TOPIC CREATE <t> <P> <R> [retention_bytes]`, `PRODUCE <t> <0|1|ALL> [clave]`,
`CONSUME <t> <grupo> [FROM_BEGINNING]`, `TOPIC LIST`, `TOPIC DESCRIBE <t>`,
`GROUP DESCRIBE <g>`, `BROKER PAUSE|RESUME <id>` y `SHUTDOWN`.

Un `produce` puede llegar a cualquier broker: si no es el líder de la partición, lo
reenvía al líder por MPI. Con `acks=0` no se imprime offset.

**Retención.** Con `retention.bytes` cada partición se guarda en segmentos de
`retention.bytes / 4` (entre 4 KiB y 64 MiB) y se borran segmentos completos, del más
viejo al más nuevo, nunca el activo ni datos en o sobre el HWM. Los offsets no se
reutilizan. Un consumidor que pide un offset ya borrado **continúa desde el primer
offset disponible**.

**Commits.** Los offsets confirmados de cada grupo se agregan a `commits.log` del
coordinador del grupo (`FNV-1a(grupo) mod N`). Se hace `fflush` en cada commit y
`fsync` en el apagado ordenado, y se recargan al arrancar. No se replican:
con el coordinador en pausa el grupo no avanza hasta que se reanude (limitación
documentada, igual que el controlador de metadatos).

## Pruebas

```bash
make test             # unitarias + capa MPI + integración + clúster
make test-mpi         # solo la capa MPI: mpirun -np 3 bin/test_mpi_layer
make test-integration # solo clientes y grupos: bash tests/test_integration.sh
make test-cluster     # solo el clúster: bash tests/test_cluster.sh
make test-race        # la carrera con -DSIN_PROTECCION y sin ella
```

`make test MPIRUN=mpiexec MPIRUN_ARGS=--oversubscribe` cambia el lanzador.
`tests/test_cluster.sh` arranca su propio clúster en puertos libres y verifica:
difusión de metadatos, reenvío al líder, orden y offsets por partición, productores
concurrentes en varios brokers, visibilidad (nada en o sobre el HWM), durabilidad con
`acks=all` y un broker en pausa, salida y regreso al ISR, rechazo de escrituras con el
líder en pausa, apagado ordenado, consistencia byte a byte de las réplicas hasta el
HWM (con `bin/mb_logdump`) y persistencia tras reiniciar. `tests/test_integration.sh` cubre el
flujo completo de clientes y grupos, solicitudes mal formadas, 60 productores en la
misma partición, particionado por clave, rebalanceo bajo carga sin dos dueños, al
menos una vez, la sintaxis de `mbcli` del enunciado, `retention.bytes` en el broker y
la persistencia de los commits de los grupos tras reiniciar. Variables: `NP`,
`BROKER_HOSTS`, `MPIRUN_ARGS`, `DATA_DIR`, `KEEP_DATA=1` (ver el encabezado del script).

## Métricas

```bash
bash bench/run.sh                 # todos los escenarios, 5 repeticiones por punto
bash bench/run.sh brokers hot     # solo algunos
python3 bench/analyze.py all      # medianas, speedup, eficiencia, Karp-Flatt y Amdahl
python3 bench/plot.py             # gráficos SVG en bench/results/plots/
```

| Escenario | Qué varía | Métrica |
| :--- | :--- | :--- |
| `brokers` | N brokers con N particiones (R=1, acks=1) | speedup por rendimiento de producción |
| `partitions` | P particiones con 3 brokers | speedup por rendimiento de producción |
| `hot` | % de mensajes con la misma clave | rendimiento relativo |
| `consumers` | C miembros de un grupo (incluye más miembros que particiones) | speedup por rendimiento de consumo |
| `replication` | `acks` 0/1/ALL × R 1..3, productores contra el líder | rendimiento y latencia relativos |

Los CSV crudos quedan en `bench/results/bench_<ambiente>_<fecha>.csv` y el resumen en
`..._summary.csv`. Con varios ambientes (la máquina local y Kabré), `analyze.py` y
`plot.py` los comparan en las mismas tablas y gráficos. En Kabré:
`scripts/kabre/kabre.sh bench`.

## Variables de entorno del broker

| Variable | Por omisión | Efecto |
| :--- | :--- | :--- |
| `MB_REPLICA_TIMEOUT_MS` | 3000 | Tiempo sin fetch tras el cual una réplica sale del ISR (y sin heartbeat tras el cual un broker se considera caído). Mínimo 1000. Con Open MPI se pasa a todos los brokers con `mpirun -x MB_REPLICA_TIMEOUT_MS`. |

## Decisiones (resumen)

- **`MPI_THREAD_MULTIPLE`**: un hilo receptor bloqueado en `MPI_Probe`/`MPI_Recv`
  despacha por tag y un hilo emisor vacía una cola acotada; cualquier hilo encola.
  `FUNNELED` obligaría a sondear con `MPI_Iprobe` (espera activa).
- **Replicación pull**: cada réplica pide `FETCH(desde = su LEO)`; eso le dice al líder
  hasta dónde tiene. HWM = mínimo LEO del ISR. Cuando no hay datos nuevos la réplica
  duerme en una variable de condición hasta que el líder le envía `FETCH_WAKE` al
  agregar, con un fetch de mantenimiento cada 500 ms que la mantiene en el ISR.
- **Sin elección de líder**: una partición cuyo líder está en pausa rechaza escrituras
  hasta que vuelva.
- **CPU del receptor**: el `MPI_Probe` bloqueante de Open MPI gira dentro de la
  biblioteca; `OMPI_MCA_mpi_yield_when_idle=1` hace que ese giro ceda el procesador
  cuando hay varios brokers en una máquina.

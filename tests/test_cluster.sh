#!/usr/bin/env bash
# Cluster tests: start a real cluster with mpirun and check the guarantees that
# depend on MPI, replication, pause/resume and the ordered shutdown.
#
#   bash tests/test_cluster.sh            (make test-cluster runs it)
#
# Covers: metadata diffusion, produce forwarded to the leader, order and offsets
# per partition, concurrent producers on several brokers, visibility (nothing at
# or above the HWM), durability with acks=all while a broker is paused, ISR exit
# and return, writes rejected while a leader is paused, ordered shutdown, replica
# consistency byte by byte up to the HWM, and persistence across a restart.
#
# Environment (all optional):
#   NP            brokers to start (default 3, minimum 3)
#   BASE_PORT     first TCP port; broker r listens on BASE_PORT + r
#   MPIRUN        launcher (default mpirun)
#   MPIRUN_ARGS   extra launcher arguments, e.g. "--map-by node" or "--oversubscribe"
#   MPIRUN_ENV    how the launcher forwards a variable (default Open MPI's "-x")
#   BROKER_HOSTS  host of each rank, space separated, for several nodes
#                 (default: 127.0.0.1 for every rank)
#   DATA_DIR      data directory; with several nodes it must be on a shared
#                 filesystem, because the consistency check reads every broker's log
#   KEEP_DATA=1   keep DATA_DIR, the broker log and the client outputs at the end

set -u

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

NP=${NP:-3}
TEST_NAME=mb_cluster
# A paused replica leaves the ISR after this long; the visibility check reads the
# HWM before that happens.
export MB_REPLICA_TIMEOUT_MS=${MB_REPLICA_TIMEOUT_MS:-4000}
ISR_WAIT=$(( MB_REPLICA_TIMEOUT_MS / 1000 + 6 ))
if [ "$NP" -lt 3 ]; then
    echo "test_cluster needs NP >= 3" >&2
    exit 2
fi
# shellcheck source=tests/cluster_lib.sh
source tests/cluster_lib.sh
if [ ! -x "$DUMP" ]; then
    echo "missing $DUMP: run make first" >&2
    exit 2
fi


echo "═══ test_cluster · $NP brokers with $MPIRUN, ports $BASE_PORT-$((BASE_PORT + NP - 1))"
echo "    data: $DATA_DIR   replica timeout: ${MB_REPLICA_TIMEOUT_MS} ms"

# --------------------------------------------------------------------------
section "arranque" "cada broker atiende en su puerto"
check "los $NP brokers responden" start_cluster
if [ -z "$MPI_PID" ] || ! kill -0 "$MPI_PID" 2>/dev/null; then
    echo "the cluster did not start; log:"
    cat "$BROKER_LOG"
    exit 1
fi

# --------------------------------------------------------------------------
section "metadatos" "topic create se reenvía al controlador y se difunde a todos"
cli 1 TOPIC CREATE orders 3 3 > "$WORK/create.out" 2>&1
check "topic create desde un broker que no es el controlador" grep -q "topic created" "$WORK/create.out"
same_layout() {
    local r ref
    ref=$(cli 0 TOPIC DESCRIBE orders 2>/dev/null | awk '$1 == "partition" {print $2, $3, $4}')
    [ -n "$ref" ] || return 1
    for ((r = 1; r < NP; r++)); do
        [ "$(cli "$r" TOPIC DESCRIBE orders 2>/dev/null | awk '$1 == "partition" {print $2, $3, $4}')" = "$ref" ] || return 1
    done
}
check "todos los brokers ven los mismos líderes y réplicas" wait_until 5 same_layout
check "crear un tópico existente falla" bash -c "! timeout 20 $CLI $(host_of 2) $((BASE_PORT + 2)) TOPIC CREATE orders 1 1 >/dev/null 2>&1"
check "replicación mayor que N falla" bash -c "! timeout 20 $CLI $(host_of 0) $BASE_PORT TOPIC CREATE toolarge 1 $((NP + 1)) >/dev/null 2>&1"

declare -a KEY LEADER
for p in 0 1 2; do
    KEY[$p]=$(key_for 3 "$p")
    LEADER[$p]=$(field 0 orders "$p" leader)
done

# --------------------------------------------------------------------------
section "produce acks=all" "reenvío al líder por MPI, orden y offsets consecutivos"
for p in 0 1 2; do
    via=$(( (LEADER[p] + 1) % NP )) # never the leader: the produce is forwarded
    seq 1 100 | sed "s/^/orders-p$p-/" | produce "$via" orders ALL "${KEY[$p]}" > "$WORK/p$p.out" 2>&1
    check "partición $p: 100 mensajes confirmados en orden (offsets 0..99)" offsets_contiguous "$WORK/p$p.out" "$p" 0 100
done

seq 1 20 | sed 's/^/orders-acks1-/' | produce 0 orders 1 "${KEY[0]}" > "$WORK/acks1.out" 2>&1
check "acks=1 confirma offsets 100..119 en la partición 0" offsets_contiguous "$WORK/acks1.out" 0 100 20
seq 1 20 | sed 's/^/orders-acks0-/' | produce 1 orders 0 "${KEY[0]}" > "$WORK/acks0.out" 2>&1
check "acks=0 no imprime offsets" bash -c "! grep -q offset= '$WORK/acks0.out'"
hwm_is() { [ "$(field 0 orders "$1" hwm)" = "$2" ] && [ "$(field 0 orders "$1" leo)" = "$2" ]; }
check "acks=0 también queda replicado (HWM = LEO = 140)" wait_until 10 hwm_is 0 140

# --------------------------------------------------------------------------
section "concurrencia" "6 productores en 3 brokers sobre la misma partición"
for i in 0 1 2 3 4 5; do
    seq 1 50 | sed "s/^/conc-$i-/" | produce $((i % NP)) orders ALL "${KEY[1]}" > "$WORK/conc$i.out" 2>&1 &
done
wait_jobs() { local j; for j in $(jobs -p); do [ "$j" = "$MPI_PID" ] || wait "$j"; done; }
wait_jobs
cat "$WORK"/conc*.out | grep '^partition=1 ' | sed 's/.*offset=//' | sort -n > "$WORK/conc.offsets"
check "300 confirmaciones" [ "$(wc -l < "$WORK/conc.offsets")" -eq 300 ]
check "offsets únicos y sin huecos (100..399)" [ "$(tr '\n' ' ' < "$WORK/conc.offsets")" = "$(seq 100 399 | tr '\n' ' ')" ]

# --------------------------------------------------------------------------
section "visibilidad" "un consumidor nunca ve mensajes en o sobre el HWM"
cli 0 TOPIC CREATE vis 1 3 >/dev/null 2>&1
wait_until 5 bash -c "[ -n \"\$(timeout 5 $CLI $(host_of 2) $((BASE_PORT + 2)) TOPIC DESCRIBE vis 2>/dev/null | grep leader=)\" ]"
VLEADER=$(field 0 vis 0 leader)
VPAUSED=$(field 0 vis 0 replicas | tr ',' '\n' | grep -vx "$VLEADER" | head -n 1)
seq 1 10 | sed 's/^/vis-/' | produce "$VLEADER" vis ALL - > /dev/null 2>&1
check "10 mensajes replicados antes de la pausa" wait_until 5 bash -c "[ \"\$(timeout 5 $CLI $(host_of "$VLEADER") $((BASE_PORT + VLEADER)) TOPIC DESCRIBE vis | grep -o 'hwm=[0-9]*')\" = hwm=10 ]"
cli "$VLEADER" BROKER PAUSE "$VPAUSED" > /dev/null 2>&1
echo "vis-after-pause" | produce "$VLEADER" vis 1 - > "$WORK/vis.out" 2>&1
check "acks=1 confirma el offset 10 con una réplica en pausa" grep -q "offset=10" "$WORK/vis.out"
check "el HWM no pasa a la réplica en pausa (hwm=10, leo=11)" bash -c "[ \"\$(timeout 5 $CLI $(host_of "$VLEADER") $((BASE_PORT + VLEADER)) TOPIC DESCRIBE vis | grep -o 'hwm=[0-9]* leo=[0-9]*')\" = 'hwm=10 leo=11' ]"
G1=$(group_avoiding "$VPAUSED" visA)
consume "$VLEADER" vis "$G1" 1.5 > "$WORK/vis1.out"
check "el consumidor recibe exactamente los 10 confirmados" [ "$(grep -c '^partition=0 ' "$WORK/vis1.out")" -eq 10 ]
check "y ninguno en o sobre el HWM" bash -c "! grep -q 'offset=10 ' '$WORK/vis1.out'"
isr_lacks() { [ -n "$(field "$1" "$2" "$3" isr)" ] && ! echo ",$(field "$1" "$2" "$3" isr)," | grep -q ",$4,"; }
check "la réplica en pausa sale del ISR por tiempo" wait_until "$ISR_WAIT" isr_lacks "$VLEADER" vis 0 "$VPAUSED"
check "sin ella el HWM avanza hasta el LEO (11)" wait_until 5 bash -c "[ \"\$(timeout 5 $CLI $(host_of "$VLEADER") $((BASE_PORT + VLEADER)) TOPIC DESCRIBE vis | grep -o 'hwm=[0-9]*')\" = hwm=11 ]"
G2=$(group_avoiding "$VPAUSED" visB)
consume "$VLEADER" vis "$G2" 1.5 > "$WORK/vis2.out"
check "ahora el mensaje 10 es visible" grep -q 'offset=10 vis-after-pause' "$WORK/vis2.out"

# --------------------------------------------------------------------------
section "durabilidad" "acks=all sigue confirmando con el broker $VPAUSED en pausa"
for p in 0 1 2; do
    [ "${LEADER[$p]}" -eq "$VPAUSED" ] && continue
    via=$(( (VPAUSED + 1 + p) % NP ))
    [ "$via" -eq "$VPAUSED" ] && via=$(( (via + 1) % NP ))
    before=$(field "${LEADER[$p]}" orders "$p" leo)
    if echo ",$(field 0 orders "$p" replicas)," | grep -q ",$VPAUSED,"; then
        wait_until "$ISR_WAIT" isr_lacks "${LEADER[$p]}" orders "$p" "$VPAUSED" > /dev/null
    fi
    seq 1 50 | sed "s/^/paused-p$p-/" | produce "$via" orders ALL "${KEY[$p]}" > "$WORK/dur$p.out" 2>&1
    check "partición $p (líder ${LEADER[$p]}): 50 confirmados desde el offset $before" offsets_contiguous "$WORK/dur$p.out" "$p" "$before" 50
done
for p in 0 1 2; do
    [ "${LEADER[$p]}" -ne "$VPAUSED" ] && continue
    via=$(( (VPAUSED + 1) % NP ))
    start=$SECONDS
    echo "to-paused-leader" | produce "$via" orders ALL "${KEY[$p]}" > "$WORK/rej$p.out" 2>&1
    rc=$?
    check "partición $p: su líder está en pausa y la escritura se rechaza" [ "$rc" -ne 0 ]
    check "el rechazo es rápido (menos de 3 s)" [ $((SECONDS - start)) -lt 3 ]
done

# --------------------------------------------------------------------------
section "reanudación" "el broker $VPAUSED se pone al día y vuelve al ISR"
cli "$VLEADER" BROKER RESUME "$VPAUSED" > /dev/null 2>&1
isr_full() {
    local isr; isr=$(field "$1" "$2" "$3" isr)
    [ "$(echo "$isr" | tr ',' '\n' | grep -c .)" -eq 3 ] && [ "$(field "$1" "$2" "$3" hwm)" = "$(field "$1" "$2" "$3" leo)" ]
}
for p in 0 1 2; do
    check "orders/$p: ISR completo de nuevo y HWM = LEO" wait_until 15 isr_full 0 orders "$p"
done
check "vis/0: ISR completo de nuevo y HWM = LEO" wait_until 15 isr_full 0 vis 0
seq 1 30 | sed 's/^/after-resume-/' | produce "$VPAUSED" orders ALL "${KEY[2]}" > "$WORK/resumed.out" 2>&1
check "el broker reanudado vuelve a atender productores (30 confirmados)" [ "$(grep -c offset= "$WORK/resumed.out")" -eq 30 ]
for p in 0 1 2; do wait_until 10 isr_full 0 orders "$p" > /dev/null; done

declare -a HWM_BEFORE
for p in 0 1 2; do HWM_BEFORE[$p]=$(field 0 orders "$p" hwm); done
VIS_HWM=$(field 0 vis 0 hwm)

# --------------------------------------------------------------------------
section "apagado ordenado" "shutdown llega a todos, guarda todo y libera MPI"
check "shutdown desde el broker 2 termina el clúster" stop_cluster 2
check "mpirun sale con código 0" [ "$STOP_RC" -eq 0 ]
check "los $NP brokers reportan que se detuvieron" [ "${STOPPED:-0}" -eq "$NP" ]
no_brokers_left() { ! pgrep -f -- "brokerd --base-port $BASE_PORT " >/dev/null; }
check "no queda ningún brokerd de esta prueba" no_brokers_left

# --------------------------------------------------------------------------
section "consistencia de réplicas" "los logs del ISR son idénticos byte a byte hasta el HWM"
consistent() {
    local topic=$1 p=$2 hwm=$3 r ref="" dir
    for ((r = 0; r < NP; r++)); do
        dir="$DATA_DIR/broker$r/topics/$topic/$p"
        [ -d "$dir" ] || continue
        "$DUMP" "$dir" leo "$hwm" > "$WORK/dump.$topic.$p.$r" 2>/dev/null || return 1
        # Only the records: the header line has each replica's own HWM/LEO.
        tail -n +2 "$WORK/dump.$topic.$p.$r" > "$WORK/rec.$topic.$p.$r"
        [ "$(wc -l < "$WORK/rec.$topic.$p.$r")" -eq "$hwm" ] || return 1
        if [ -z "$ref" ]; then ref="$WORK/rec.$topic.$p.$r"; else cmp -s "$ref" "$WORK/rec.$topic.$p.$r" || return 1; fi
    done
    [ -n "$ref" ]
}
same_segments() {
    local topic=$1 p=$2 r f base="$DATA_DIR/broker$3/topics/$1/$2"
    for ((r = 0; r < NP; r++)); do
        [ "$r" -eq "$3" ] && continue
        for f in "$base"/*.log; do
            cmp -s "$f" "$DATA_DIR/broker$r/topics/$topic/$p/$(basename "$f")" || return 1
        done
    done
}
for p in 0 1 2; do
    check "orders/$p: las 3 réplicas tienen los mismos ${HWM_BEFORE[$p]} registros" consistent orders "$p" "${HWM_BEFORE[$p]}"
    check "orders/$p: y sus archivos de segmento son idénticos" same_segments orders "$p" "${LEADER[$p]}"
done
check "vis/0: las 3 réplicas tienen los mismos $VIS_HWM registros" consistent vis 0 "$VIS_HWM"

# --------------------------------------------------------------------------
section "persistencia" "tras reiniciar, el clúster conserva tópicos, logs y HWM"
check "el clúster arranca de nuevo sobre los mismos datos" start_cluster
for p in 0 1 2; do
    check "orders/$p: HWM conservado (${HWM_BEFORE[$p]})" wait_until 10 bash -c "[ \"\$(timeout 5 $CLI $(host_of 0) $BASE_PORT TOPIC DESCRIBE orders | awk '\$1==\"partition\" && \$2==$p' | grep -o 'hwm=[0-9]*')\" = hwm=${HWM_BEFORE[$p]} ]"
done
G3=$(group_avoiding -1 restart)
consume 1 orders "$G3" 4 > "$WORK/restart.out"
for p in 0 1 2; do
    check "orders/$p: un consumidor nuevo lee los ${HWM_BEFORE[$p]} mensajes en orden" \
        offsets_contiguous "$WORK/restart.out" "$p" 0 "${HWM_BEFORE[$p]}"
done
check "todos los mensajes acks=all del inicio siguen ahí" bash -c "[ \$(grep -c 'orders-p[0-2]-' '$WORK/restart.out') -eq 300 ]"
echo "after-restart" | produce 2 orders ALL "${KEY[0]}" > "$WORK/after.out" 2>&1
check "los offsets continúan, no se reutilizan (${HWM_BEFORE[0]})" grep -q "offset=${HWM_BEFORE[0]}$" "$WORK/after.out"
check "segundo apagado ordenado" stop_cluster 0
check "mpirun sale con código 0" [ "$STOP_RC" -eq 0 ]

summary test_cluster

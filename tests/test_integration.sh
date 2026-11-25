#!/usr/bin/env bash
# Client and consumer-group integration tests against a real cluster of 3 brokers.
#
#   bash tests/test_integration.sh            (make test-integration runs it)
#
# Covers: the full flow (create, produce, consume, commit, group describe),
# malformed requests and abrupt disconnects, 60 producers on the same partition,
# same key -> same partition, rebalancing under load with no partition owned by
# two members at once, at-least-once delivery when a consumer is killed, the
# command syntax of the assignment, retention.bytes in the broker, and group
# commits that survive an orderly restart.
# Environment: see tests/test_cluster.sh (NP, BASE_PORT, MPIRUN, MPIRUN_ARGS, ...).

set -u

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

NP=${NP:-3}
TEST_NAME=mb_integration
# shellcheck source=tests/cluster_lib.sh
source tests/cluster_lib.sh

# raw <rank> <bytes>: sends exactly those bytes (printf format) on a new connection
# and prints the first line of the answer, or "closed" if there is none.
raw() {
    local rank=$1 payload=$2 line=""
    exec 7<>"/dev/tcp/$(host_of "$rank")/$((BASE_PORT + rank))" || { echo "noconnect"; return; }
    # shellcheck disable=SC2059
    printf "$payload" >&7 2>/dev/null
    if IFS= read -r -t 5 line <&7; then echo "$line"; else echo "closed"; fi
    exec 7>&-
}

# Prefixes every line with the time it was read (bash builtin, no process per line).
stamp() {
    local line
    while IFS= read -r line; do printf '%s %s\n' "$EPOCHREALTIME" "$line"; done
}

echo "═══ test_integration · $NP brokers with $MPIRUN, ports $BASE_PORT-$((BASE_PORT + NP - 1))"

section "arranque" "clúster de $NP brokers"
check "los $NP brokers responden" start_cluster
if [ -z "$MPI_PID" ] || ! kill -0 "$MPI_PID" 2>/dev/null; then
    echo "the cluster did not start; log:"
    cat "$BROKER_LOG"
    exit 1
fi

# --------------------------------------------------------------------------
section "flujo completo" "crear, producir, consumir, confirmar y describir el grupo"
cli 0 TOPIC CREATE flow 3 2 > "$WORK/create.out" 2>&1
check "topic create" grep -q "topic created" "$WORK/create.out"
check "topic list lo muestra en otro broker" wait_until 5 bash -c "timeout 5 $CLI $(host_of 2) $((BASE_PORT + 2)) TOPIC LIST | grep -q '^flow '"
seq 1 30 | sed 's/^/flow-/' | produce 1 flow ALL - > "$WORK/flow.out" 2>&1
check "30 mensajes sin clave confirmados con acks=all" [ "$(grep -c offset= "$WORK/flow.out")" -eq 30 ]
check "sin clave se reparten en las 3 particiones" [ "$(cut -d' ' -f1 "$WORK/flow.out" | sort -u | wc -l)" -eq 3 ]
consume 2 flow billing 3 > "$WORK/flow_consumed.out"
check "el consumidor recibe los 30, cada uno una vez" [ "$(grep -o 'flow-[0-9]*' "$WORK/flow_consumed.out" | sort -u | wc -l)" -eq 30 ]
cli 0 GROUP DESCRIBE billing > "$WORK/describe.out" 2>&1
lag_zero() { [ "$(grep -c 'lag 0$' "$WORK/describe.out")" -eq 3 ]; }
check "group describe: offsets confirmados y lag 0 en las 3 particiones" lag_zero
cli 1 GROUP DESCRIBE billing > "$WORK/describe1.out" 2>&1
check "group describe da lo mismo desde otro broker" cmp -s "$WORK/describe.out" "$WORK/describe1.out"

# --------------------------------------------------------------------------
section "solicitudes mal formadas" "ningún broker se cae ni deja de atender"
check "comando desconocido → ERR" [ "$(raw 0 'HELLO world\n' | cut -d' ' -f1)" = ERR ]
check "PRODUCE con argumentos de menos → ERR" [ "$(raw 1 'PRODUCE flow 1 0\n' | cut -d' ' -f1)" = ERR ]
check "acks inválido → ERR" [ "$(raw 2 'PRODUCE flow 7 0 5\nhello' | cut -d' ' -f1)" = ERR ]
check "valor más grande que el máximo → ERR" [ "$(raw 0 'PRODUCE flow 1 0 999999999\n' | cut -d' ' -f1)" = ERR ]
check "clave más grande que el máximo → ERR" [ "$(raw 0 'PRODUCE flow 1 99999 5\n' | cut -d' ' -f1)" = ERR ]
check "tópico inexistente → ERR" [ "$(raw 1 'PRODUCE nosuchtopic 1 0 5\nhello' | cut -d' ' -f1)" = ERR ]
long=$(printf 'x%.0s' $(seq 1 700))
check "línea más larga que el límite → ERR" [ "$(raw 2 "TOPIC DESCRIBE $long\n" | cut -d' ' -f1)" = ERR ]
check "COMMIT de un miembro que no existe → ERR" [ "$(raw 0 'COMMIT flow billing 99 0 5\n' | cut -d' ' -f1)" = ERR ]
check "bytes binarios → ERR o cierre" bash -c "r=\$(exec 7<>/dev/tcp/$(host_of 1)/$((BASE_PORT + 1)); printf '\\x01\\xff\\xfe\\n' >&7; read -r -t 5 l <&7 && echo \"\$l\" || echo closed); [ \"\${r%% *}\" = ERR ] || [ \"\$r\" = closed ]"
# Abrupt disconnects: a body cut in the middle, and connections closed at once.
for r in 0 1 2; do
    port=$((BASE_PORT + r))
    (exec 7<>"/dev/tcp/$(host_of "$r")/$port"; printf 'PRODUCE flow 1 0 100\n0123456789' >&7) 2>/dev/null
    for _ in $(seq 10); do (exec 7<>"/dev/tcp/$(host_of "$r")/$port") 2>/dev/null; done
done
check "los $NP brokers siguen respondiendo" all_answer
echo "still-works" | produce 0 flow 1 - > "$WORK/after_garbage.out" 2>&1
check "y siguen aceptando mensajes" grep -q offset= "$WORK/after_garbage.out"
check "el proceso del clúster sigue vivo" kill -0 "$MPI_PID"

# --------------------------------------------------------------------------
section "estrés" "60 productores a la vez sobre la misma partición"
cli 0 TOPIC CREATE stress 3 2 >/dev/null 2>&1
wait_until 5 bash -c "timeout 5 $CLI $(host_of 2) $((BASE_PORT + 2)) TOPIC DESCRIBE stress | grep -q leader="
pids=()
for i in $(seq 1 60); do
    seq 1 20 | sed "s/^/stress-$i-/" | produce $((i % NP)) stress 1 samekey > "$WORK/stress.$i" 2>&1 &
    pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done
cat "$WORK"/stress.* > "$WORK/stress.all"
check "1200 confirmaciones" [ "$(grep -c offset= "$WORK/stress.all")" -eq 1200 ]
check "todas en la misma partición" [ "$(cut -d' ' -f1 "$WORK/stress.all" | sort -u | wc -l)" -eq 1 ]
sed 's/.*offset=//' "$WORK/stress.all" | sort -n > "$WORK/stress.offsets"
check "offsets únicos y sin huecos (0..1199)" [ "$(tr '\n' ' ' < "$WORK/stress.offsets")" = "$(seq 0 1199 | tr '\n' ' ')" ]

# --------------------------------------------------------------------------
section "particionado por clave" "misma clave → misma partición, desde cualquier broker"
same_partition_ok=1
expected_ok=1
for k in $(seq 1 10); do
    key="customer$k"
    for r in 0 1 2; do echo "$key-$r" | produce "$r" flow 1 "$key"; done > "$WORK/key.$k" 2>&1
    [ "$(cut -d' ' -f1 "$WORK/key.$k" | sort -u | wc -l)" -eq 1 ] || same_partition_ok=0
    [ "$(head -n 1 "$WORK/key.$k" | cut -d' ' -f1)" = "partition=$(( $(fnv1a "$key") % 3 ))" ] || expected_ok=0
done
check "10 claves: cada una siempre cae en la misma partición" [ "$same_partition_ok" -eq 1 ]
check "y es FNV-1a(clave) mod P" [ "$expected_ok" -eq 1 ]

# --------------------------------------------------------------------------
section "rebalanceo bajo carga" "entran y salen miembros mientras se produce; al menos una vez"
cli 0 TOPIC CREATE orders 4 2 >/dev/null 2>&1
wait_until 5 bash -c "timeout 5 $CLI $(host_of 2) $((BASE_PORT + 2)) TOPIC DESCRIBE orders | grep -q leader="
GROUP=shipping
# Steady load for about 12 s: one message every 10 ms.
(for i in $(seq 1 1200); do echo "order-$i"; sleep 0.01; done) | produce 0 orders 1 - > "$WORK/orders.produced" 2>&1 &
PRODUCER=$!
start_member() { # start_member <name> <rank>
    "$CLI" "$(host_of "$2")" $((BASE_PORT + $2)) CONSUME orders "$GROUP" FROM_BEGINNING \
        > >(stamp > "$WORK/member.$1") 2>/dev/null &
    eval "PID_$1=\$!"
}
start_member A 0
sleep 2
start_member B 1
sleep 2
start_member C 2
sleep 2
# shellcheck disable=SC2154
{ kill -KILL "$PID_A"; wait "$PID_A"; } 2>/dev/null   # dies without LEAVE or its last commit
wait "$PRODUCER"
sleep 4               # B and C drain what is left
cli 1 GROUP DESCRIBE "$GROUP" > "$WORK/orders.describe" 2>&1
# shellcheck disable=SC2154
kill -TERM "$PID_B" "$PID_C" 2>/dev/null
wait "$PID_B" "$PID_C" 2>/dev/null
sleep 0.5

grep -o 'partition=[0-9]* offset=[0-9]*' "$WORK/orders.produced" | sort -u > "$WORK/orders.confirmed"
cat "$WORK"/member.? | grep -o 'partition=[0-9]* offset=[0-9]*' | sort -u > "$WORK/orders.delivered"
check "1200 mensajes confirmados" [ "$(wc -l < "$WORK/orders.confirmed")" -eq 1200 ]
check "al menos una vez: cada confirmado llegó a algún miembro" bash -c "[ -z \"\$(comm -23 '$WORK/orders.confirmed' '$WORK/orders.delivered')\" ]"
check "el miembro eliminado recibió mensajes antes de morir" [ -s "$WORK/member.A" ]
check "los dos que quedan comparten las 4 particiones" bash -c "[ \$(grep -c '^  partition' '$WORK/orders.describe') -eq 4 ] && [ \$(grep '^  partition' '$WORK/orders.describe' | awk '{print \$4}' | sort -u | grep -vc '^-$') -eq 2 ]"
check "y quedaron al día (lag 0)" [ "$(grep -c 'lag 0$' "$WORK/orders.describe")" -eq 4 ]
# Exclusive ownership: merge every delivery by the time it was read and count, per
# partition, how often the receiving member changes. Each membership change (A, B
# and C joining, A dying) can move a partition once; two owners at the same time
# would interleave their deliveries and switch members on almost every batch.
switches=$(for m in A B C; do sed -n "s/^\([0-9.]*\) partition=\([0-9]*\) .*/\1 \2 $m/p" "$WORK/member.$m"; done \
    | sort -n | awk '{ if (($2 in last) && last[$2] != $3) n[$2]++; last[$2] = $3 }
                     END { m = 0; for (p in n) if (n[p] > m) m = n[p]; print m + 0 }')
check "ninguna partición con dos dueños a la vez (cambios de dueño por partición: $switches ≤ 4)" [ "$switches" -le 4 ]

# --------------------------------------------------------------------------
section "sintaxis del enunciado" "mbcli host:port con comandos en minúscula y opciones -p -r -k --acks -g --from-beginning"
ADDR1="$(host_of 1):$((BASE_PORT + 1))"
ADDR2="$(host_of 2):$((BASE_PORT + 2))"
timeout 20 "$CLI" "$ADDR1" topic create pedidos -p 3 -r 2 > "$WORK/spec_create.out" 2>&1
check "topic create <t> -p 3 -r 2" grep -q "topic created" "$WORK/spec_create.out"
check "topic describe <t> muestra las 3 particiones" wait_until 5 bash -c "[ \$(timeout 5 $CLI $ADDR2 topic describe pedidos | grep -c '^  partition .* leader=') -eq 3 ]"
printf 'pedido 1\npedido 2\n' | timeout 30 "$CLI" "$ADDR2" produce pedidos -k cliente42 --acks all > "$WORK/spec_produce.out" 2>&1
check "produce -k cliente42 --acks all confirma 2 mensajes en la misma partición" \
    bash -c "[ \$(grep -c offset= '$WORK/spec_produce.out') -eq 2 ] && [ \$(cut -d' ' -f1 '$WORK/spec_produce.out' | sort -u | wc -l) -eq 1 ]"
timeout 4 "$CLI" "$ADDR1" consume pedidos -g facturacion --from-beginning > "$WORK/spec_consume.out" 2>/dev/null
check "consume -g --from-beginning recibe los 2" [ "$(grep -c 'pedido [12]$' "$WORK/spec_consume.out")" -eq 2 ]
timeout 10 "$CLI" "$ADDR2" group describe facturacion > "$WORK/spec_group.out" 2>&1
check "group describe muestra el commit (lag 0 en su partición)" grep -q "committed 2 lag 0$" "$WORK/spec_group.out"
check "una opción desconocida es un error de uso" bash -c "! timeout 5 $CLI $ADDR1 produce pedidos --nope 1 </dev/null >/dev/null 2>&1"

# --------------------------------------------------------------------------
section "retención" "retention.bytes en el broker: se borran los mensajes viejos y los offsets no se reutilizan"
cli 0 TOPIC CREATE ret 1 2 16384 > "$WORK/ret_create.out" 2>&1
check "topic create con retention.bytes = 16384" grep -q "topic created" "$WORK/ret_create.out"
value=$(printf '%0100d' 0)
seq 0 599 | sed "s/^/ret-/; s/\$/-$value/" | produce 1 ret 1 - > "$WORK/ret.out" 2>&1
check "600 mensajes de ~110 B confirmados (offsets 0..599)" offsets_contiguous "$WORK/ret.out" 0 0 600
consume 2 ret retgroup 4 > "$WORK/ret_consumed.out"
ret_first=$(sed -n '1s/.*offset=\([0-9]*\) .*/\1/p' "$WORK/ret_consumed.out")
check "los primeros offsets ya no existen (el primero disponible es ${ret_first:-?} > 0)" [ "${ret_first:-0}" -gt 0 ]
check "y se lee del primero disponible al último sin huecos" \
    [ "$(sed -n 's/.*offset=\([0-9]*\) .*/\1/p' "$WORK/ret_consumed.out" | tr '\n' ' ')" = "$(seq "${ret_first:-0}" 599 | tr '\n' ' ')" ]
echo "ret-after" | produce 0 ret 1 - > "$WORK/ret_after.out" 2>&1
check "el siguiente offset es 600: no se reutilizan" grep -q "offset=600$" "$WORK/ret_after.out"

# --------------------------------------------------------------------------
section "persistencia de commits" "tras apagar y reiniciar, el grupo conserva sus offsets confirmados"
cli 0 GROUP DESCRIBE billing | grep '^  partition' | awk '{print $2, $6}' > "$WORK/commits_before.out"
check "hay commits antes del reinicio" [ "$(wc -l < "$WORK/commits_before.out")" -eq 3 ]
check "shutdown ordenado" stop_cluster 0
check "el clúster arranca de nuevo sobre los mismos datos" start_cluster
check "group describe da los mismos offsets confirmados" \
    wait_until 10 bash -c "cmp -s '$WORK/commits_before.out' <(timeout 5 $CLI $(host_of 1) $((BASE_PORT + 1)) GROUP DESCRIBE billing | grep '^  partition' | awk '{print \$2, \$6}')"
seq 1 3 | sed 's/^/flow-new-/' | produce 0 flow ALL - > /dev/null 2>&1
consume 1 flow billing 4 > "$WORK/billing_after.out"
check "el grupo sigue desde sus commits: solo recibe los 3 nuevos" \
    bash -c "[ \$(grep -c 'flow-new-' '$WORK/billing_after.out') -eq 3 ] && ! grep -q 'flow-[0-9]' '$WORK/billing_after.out'"

# --------------------------------------------------------------------------
section "apagado" "shutdown ordenado del clúster"
check "shutdown termina el clúster" stop_cluster 1
check "mpirun sale con código 0" [ "$STOP_RC" -eq 0 ]

summary test_integration

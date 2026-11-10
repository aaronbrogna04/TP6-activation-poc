#!/usr/bin/env bash
# Reenvía un evento YA PROCESADO (mismo eventId, misma clave, mismos headers) para mostrar la idempotencia
# (RNF-03): cada consumidor lo descarta por processed_events y confirma el offset; no hay cuentas, órdenes,
# emails ni puntos duplicados.
#
#   scripts/replay-event.sh                          # el último evento de activation.requested
#   scripts/replay-event.sh -t activation.events     # el último evento de ese topic
#   scripts/replay-event.sh <eventId>                # ese evento (lo busca en los 4 topics)
#   scripts/replay-event.sh -t billing.events <eventId>
set -euo pipefail
cd "$(dirname "$0")/.."

TOPIC=""
while getopts "t:" opt; do
  case "$opt" in
    t) TOPIC="$OPTARG" ;;
    *) echo "uso: $0 [-t topic] [eventId]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))
EVENT_ID="${1:-}"

if [[ -n "$TOPIC" ]]; then TOPICS=("$TOPIC")
elif [[ -n "$EVENT_ID" ]]; then TOPICS=(activation.requested billing.events provisioning.events activation.events)
else TOPICS=(activation.requested)
fi

KAFKA=(docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx64m kafka)

for t in "${TOPICS[@]}"; do
  # Lee el topic completo (son pocos mensajes en la POC): "key<TAB>value" por línea.
  LINE="$("${KAFKA[@]}" /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic "$t" --from-beginning --timeout-ms 6000 \
      --property print.key=true --property "key.separator=	" 2>/dev/null \
    | python3 -I -c '
import json, sys
want = sys.argv[1]
best = None
for raw in sys.stdin:
    key, _, value = raw.rstrip("\n").partition("\t")
    try:
        ev = json.loads(value)
        ev["eventId"], ev["occurredAt"]
    except Exception:
        continue  # mensajes inválidos (p. ej. los de send-invalid-message.sh)
    if want and ev["eventId"] != want:
        continue
    if best is None or ev["occurredAt"] >= best[1]["occurredAt"]:
        best = (key, ev, value)
if best:
    key, ev, value = best
    # Formato de kafka-console-producer con parse.headers=true y parse.key=true
    print("eventType:%s,correlationId:%s,eventId:%s\t%s\t%s" % (ev["eventType"], ev["correlationId"], ev["eventId"], key, value))
' "$EVENT_ID")"
  if [[ -n "$LINE" ]]; then
    VALUE="$(cut -f3- <<<"$LINE")"
    echo "↻ Reenviando a ${t} (key=$(cut -f2 <<<"$LINE")):"
    echo "  ${VALUE}"
    printf '%s\n' "$LINE" | "${KAFKA[@]}" /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic "$t" \
      --property parse.headers=true --property parse.key=true
    echo "Listo. En los logs de cada consumidor: \"ya procesado (processed_events); se descarta\"."
    echo "  docker compose logs --since 30s billing-service provisioning-service notification-service crm-analytics-service activation-api | grep 'ya procesado'"
    exit 0
  fi
done

echo "No se encontró ${EVENT_ID:+el evento $EVENT_ID}${EVENT_ID:-ningún evento} en: ${TOPICS[*]}" >&2
exit 1

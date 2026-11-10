#!/usr/bin/env bash
# Publica un mensaje inválido en activation.requested para mostrar la DLQ (criterio de aceptación de la POC).
#
#   scripts/send-invalid-message.sh            # JSON roto (no se puede parsear)
#   scripts/send-invalid-message.sh contract   # JSON válido que no cumple el contrato (falta payload.channel / simulateFailure)
#
# Cada consumidor de activation.requested (billing-svc, provisioning-svc, crm-analytics) lo rechaza por su
# cuenta y publica el mensaje original en activation.requested.dlq con el motivo en los headers
# (dlq-error, dlq-service, dlq-attempts, dlq-original-topic/partition/offset). No hay reintentos: reintentar
# no arregla un mensaje mal formado. El resto del flujo sigue normalmente.
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-json}"
KEY="C-INVALID"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
case "$MODE" in
  json)
    VALUE='{"eventId":"esto-no-es-json", "eventType": ActivationRequested'
    ;;
  contract)
    ID="$(cat /proc/sys/kernel/random/uuid)"
    VALUE="{\"eventId\":\"${ID}\",\"eventType\":\"ActivationRequested\",\"version\":1,\"occurredAt\":\"${NOW}\",\"correlationId\":\"act-invalid\",\"customerId\":\"${KEY}\",\"source\":\"send-invalid-message.sh\",\"payload\":{\"planId\":\"Plan Digital\"}}"
    ;;
  *)
    echo "uso: $0 [json|contract]" >&2
    exit 2
    ;;
esac

KAFKA=(docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx64m kafka)

echo "→ activation.requested  key=${KEY}"
echo "  value=${VALUE}"
printf '%s\t%s\n' "$KEY" "$VALUE" | "${KAFKA[@]}" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic activation.requested \
  --property parse.key=true --property "key.separator=	"

echo "Esperando a que los consumidores lo rechacen..."
sleep "${WAIT_SECONDS:-4}"

echo "← activation.requested.dlq (mensajes con key=${KEY}; headers | key | value):"
"${KAFKA[@]}" /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic activation.requested.dlq --from-beginning --timeout-ms 5000 \
  --property print.headers=true --property print.key=true --property print.partition=true --property print.offset=true \
  2>/dev/null | grep -F "${KEY}" | tail -n 6 || echo "  (todavía nada; mirá Kafka UI → Topics → activation.requested.dlq)"

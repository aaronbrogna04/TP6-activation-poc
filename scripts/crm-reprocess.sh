#!/usr/bin/env bash
# RF-10: crm-analytics reprocesa el historial desde el inicio.
# Borra su proyección (event_log + processed_events, solo en la base crm), lleva los offsets del group
# crm-analytics al inicio de los 4 topics y lo vuelve a levantar. Ningún otro servicio se entera.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose stop crm-analytics-service
docker compose exec -T mongo mongosh --quiet crm --eval \
  'print("event_log borrados: " + db.event_log.deleteMany({}).deletedCount + ", processed_events borrados: " + db.processed_events.deleteMany({}).deletedCount)'
docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx64m kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group crm-analytics --reset-offsets --to-earliest \
  --topic activation.requested --topic billing.events --topic provisioning.events --topic activation.events --execute
docker compose start crm-analytics-service
echo "Reprocesando... resumen en: docker compose logs -f crm-analytics-service | grep event_log"

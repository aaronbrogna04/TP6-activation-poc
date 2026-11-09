#!/bin/bash
# Crea los 4 topics de negocio y sus DLQ (documento §05): 3 particiones, retención 7 días.
# El broker tiene auto.create.topics.enable=false: este script es el único que crea topics.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVER:-kafka:19092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION="${TOPIC_REPLICATION:-1}"
RETENTION_MS="${TOPIC_RETENTION_MS:-604800000}" # 7 días
KAFKA_BIN=/opt/kafka/bin

TOPICS=(
  activation.requested
  billing.events
  provisioning.events
  activation.events
)

echo "Esperando a Kafka en ${BOOTSTRAP}..."
until "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; do
  sleep 2
done

for t in "${TOPICS[@]}"; do
  for name in "${t}" "${t}.dlq"; do
    "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
      --topic "${name}" --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}" \
      --config retention.ms="${RETENTION_MS}"
  done
done

echo "Topics:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --describe \
  | grep -E '^Topic:' | grep -v '__consumer_offsets'

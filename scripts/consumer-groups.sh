#!/usr/bin/env bash
# Lo mismo que muestra Kafka UI → Consumers: particiones asignadas, offsets, lag y miembro de cada group.
#   scripts/consumer-groups.sh                  # todos los groups de la POC
#   scripts/consumer-groups.sh notification-svc # uno solo
set -euo pipefail
cd "$(dirname "$0")/.."
GROUPS_ARGS=(--all-groups)
[[ -n "${1:-}" ]] && GROUPS_ARGS=(--group "$1")
docker compose exec -T -e KAFKA_HEAP_OPTS=-Xmx64m kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe "${GROUPS_ARGS[@]}" 2>/dev/null \
  | grep -v '^$' | grep -v 'console-consumer' | awk '{printf "%-18s %-22s %-4s %-8s %-8s %-4s %s\n", $1, $2, $3, $4, $5, $6, $8}'

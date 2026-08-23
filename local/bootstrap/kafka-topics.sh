#!/usr/bin/env bash
# Create the Kafka topics the platform expects. Idempotent.
set -euo pipefail

BROKER_CONTAINER="${BROKER_CONTAINER:-mlp-kafka}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:19092}"

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

topic() {
  local name="$1" partitions="$2"
  say "Kafka: topic $name (partitions=$partitions)"
  docker exec "$BROKER_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 >/dev/null
}

say "waiting for broker in $BROKER_CONTAINER"
for i in $(seq 1 40); do
  if docker exec "$BROKER_CONTAINER" /opt/kafka/bin/kafka-broker-api-versions.sh \
       --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then break; fi
  [ "$i" = 40 ] && { echo "kafka broker not ready" >&2; exit 1; }
  sleep 2
done

topic mlp.events 3
topic mlp.events.dlq 1

say "Kafka topics ready"
docker exec "$BROKER_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --list

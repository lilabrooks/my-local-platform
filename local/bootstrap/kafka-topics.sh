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

# relay, the webhook delivery service. Records are keyed by tenant id, so the
# partition count is the ceiling on delivery-consumer parallelism -- KEDA will
# not scale a consumer group past it, because the extra pods would sit idle.
# Twelve leaves room for the M2 autoscaling demo; three did not.
#
# Raising this later is disruptive rather than merely awkward: --alter
# --partitions only increases, and increasing reshuffles the key-to-partition
# mapping, which breaks per-tenant ordering for records already on the log.
# Set it before there is data. See docs/adr/0006-kafka-over-sqs-for-delivery.md.
topic mlp.relay.deliveries 12
topic mlp.relay.deliveries.dlq 1

say "Kafka topics ready"
docker exec "$BROKER_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --list

#!/usr/bin/env bash
# Redeliver events that relay already processed, by moving its consumer group
# back in time.
#
# This is the property ADR 0006 chose Kafka for. It is worth noting how little
# code it takes: the log still holds the events, so replay is an offset reset
# rather than a feature relay had to build. A queue would have deleted them on
# acknowledgement and this script could not exist.
#
#   ./scripts/relay-replay.sh              # redeliver the last hour
#   SINCE=6h ./scripts/relay-replay.sh     # "resend everything from the last six hours"
#   SINCE=earliest ./scripts/relay-replay.sh
#   MODE=cluster ./scripts/relay-replay.sh # against minikube instead of compose
#
# Two modes, because the consumer is stopped by a different lever in each and
# there is no lever that works for both. MODE is detected from whichever
# consumer is actually running; set it explicitly to override. Detection
# refuses to guess when it finds both or neither, since running the compose and
# cluster consumers together splits the partitions between them -- the trap
# docs/runbook-k8s.md warns about.
#
# Kafka refuses to move a group's offsets while it has members, so the consumer
# is stopped, reset, and started again. That is not incidental -- an offset
# reset under a live consumer would race with its own commits.
set -euo pipefail

SINCE="${SINCE:-1h}"
GROUP="${RELAY_CONSUMER_GROUP:-relay-deliver}"
TOPIC="${RELAY_TOPIC:-mlp.relay.deliveries}"
BROKER_CONTAINER="${BROKER_CONTAINER:-mlp-kafka}"
BOOTSTRAP="${KAFKA_INTERNAL_BOOTSTRAP:-localhost:19092}"
COMPOSE_FILE="${COMPOSE_FILE:-local/docker-compose.yml}"
CONSUMER_SERVICE="${CONSUMER_SERVICE:-relay-deliver}"
NAMESPACE="${RELAY_NAMESPACE:-mlp}"
MODE="${MODE:-auto}"

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

compose_consumer_running() {
  [ -n "$(docker compose -f "$COMPOSE_FILE" ps -q "$CONSUMER_SERVICE" 2>/dev/null)" ]
}

cluster_consumer_present() {
  command -v kubectl >/dev/null 2>&1 &&
    kubectl -n "$NAMESPACE" get scaledobject "$CONSUMER_SERVICE" >/dev/null 2>&1
}

if [ "$MODE" = auto ]; then
  if compose_consumer_running && cluster_consumer_present; then
    echo "both the compose and cluster consumers are present. They join the SAME" >&2
    echo "group and split the partitions, so a replay would be delivered half to" >&2
    echo "each. Stop one, or set MODE=compose / MODE=cluster deliberately." >&2
    exit 1
  elif compose_consumer_running; then
    MODE=compose
  elif cluster_consumer_present; then
    MODE=cluster
  else
    echo "found no relay-deliver in compose or in namespace $NAMESPACE." >&2
    echo "Bring one up: 'make up-apps', or 'make k8s-up && make k8s-apply-local'." >&2
    exit 1
  fi
fi
say "mode: $MODE"

# The KEDA HPA restores minReplicaCount within seconds of a manual scale to
# zero, so `kubectl scale` is not a lever here -- the consumer rejoins the group
# and the reset fails. Pausing the ScaledObject is, and it is what
# docs/runbook-k8s.md documents.
PAUSE_ANNOTATION=autoscaling.keda.sh/paused-replicas

stop_consumer() {
  case "$MODE" in
    compose)
      docker compose -f "$COMPOSE_FILE" stop "$CONSUMER_SERVICE" >/dev/null
      ;;
    cluster)
      # Paused, not scaled: removing the annotation later hands scaling back to
      # KEDA rather than leaving a Deployment someone has to remember to fix.
      kubectl -n "$NAMESPACE" annotate scaledobject "$CONSUMER_SERVICE" \
        "$PAUSE_ANNOTATION=0" --overwrite >/dev/null
      ;;
  esac
  # Both modes. An interrupted run leaves the consumer stopped otherwise, and
  # the topic silently stops draining -- a stack left broken by a script that
  # looked like it merely stopped.
  trap restore_consumer EXIT INT TERM
}

# Restarts the consumer, and is also the EXIT trap set by stop_consumer.
restore_consumer() {
  case "$MODE" in
    compose)
      docker compose -f "$COMPOSE_FILE" start "$CONSUMER_SERVICE" >/dev/null
      ;;
    cluster)
      kubectl -n "$NAMESPACE" annotate scaledobject "$CONSUMER_SERVICE" \
        "$PAUSE_ANNOTATION-" >/dev/null 2>&1 || true
      ;;
  esac
}

kafka() { docker exec "$BROKER_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" "$@"; }

# GNU date and BSD date disagree about relative times, and this runs on both a
# developer's mac and an ubuntu runner. GNU rejects "-1h" outright -- it wants
# "1 hour ago" -- so the unit is expanded rather than passed through. BSD's -v
# uses M for minutes and m for MONTHS, which is worth getting right.
utc_since() {
  local spec="$1" n unit word bsd
  n="${spec%%[!0-9]*}"
  unit="${spec#"$n"}"
  if [ -z "$n" ]; then
    echo "SINCE must start with a number (got '$spec')" >&2
    return 1
  fi
  case "$unit" in
    h | hour | hours)      word=hour   bsd=H ;;
    m | min | minute | minutes) word=minute bsd=M ;;
    d | day | days)        word=day    bsd=d ;;
    *)
      echo "SINCE must look like 30m, 6h or 2d, or be 'earliest' (got '$spec')" >&2
      return 1
      ;;
  esac
  date -u -d "${n} ${word} ago" '+%Y-%m-%dT%H:%M:%S.000' 2>/dev/null \
    || date -u -v"-${n}${bsd}" '+%Y-%m-%dT%H:%M:%S.000'
}

say "stopping $CONSUMER_SERVICE so the group can be reset"
stop_consumer

# A clean shutdown leaves the group immediately; wait rather than assuming.
say "waiting for group $GROUP to go inactive"
for i in $(seq 1 30); do
  # Counted from the end: the COORDINATOR column contains a space, so it splits
  # into two fields and positions from the left do not line up. STATE is always
  # second from the right, ahead of #MEMBERS. Reading $4 gets the assignment
  # strategy, which is "-" on an empty group and never matches.
  state=$(kafka --describe --group "$GROUP" --state 2>/dev/null | awk 'NR>2 && NF {print $(NF-1)}' | head -1)
  case "$state" in
    Empty | Dead | "") break ;;
  esac
  if [ "$i" = 30 ]; then
    echo "group $GROUP is still $state after 30s; it cannot be reset while it has members" >&2
    exit 1
  fi
  sleep 1
done

if [ "$SINCE" = "earliest" ]; then
  say "resetting $GROUP to the beginning of $TOPIC"
  reset=(--to-earliest)
else
  from=$(utc_since "$SINCE")
  say "resetting $GROUP to $from UTC (last $SINCE)"
  reset=(--to-datetime "$from")
fi

# --execute prints the offsets it moved to, which is the receipt for this run.
kafka --group "$GROUP" --reset-offsets "${reset[@]}" --topic "$TOPIC" --execute 2>/dev/null \
  | awk -v t="$TOPIC" '$2 == t {printf "    partition %-3s -> offset %s\n", $3, $4}'

say "starting $CONSUMER_SERVICE"
restore_consumer
trap - EXIT INT TERM

say "replaying. every event after that point is being delivered again"
if [ "$MODE" = cluster ]; then
  echo "    watch it land:  kubectl -n $NAMESPACE port-forward svc/sink 8086:8081"
  echo "                    then: curl -s localhost:8086/received | python3 -m json.tool"
  echo "    or on the panel: make monitoring-ui"
else
  echo "    watch it land:  curl -s localhost:8084/received | python3 -m json.tool"
fi

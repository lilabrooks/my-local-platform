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
COMPOSE_FILE="${COMPOSE_FILE:-local/docker-compose.yml}"
CONSUMER_SERVICE="${CONSUMER_SERVICE:-relay-deliver}"
NAMESPACE="${RELAY_NAMESPACE:-mlp}"
MODE="${MODE:-auto}"
LOCAL_CLUSTER_CONTEXT="${MINIKUBE_PROFILE:-mlp}"

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

if [ "$MODE" = cluster ]; then
  current_context=$(kubectl config current-context)
  if [ "$current_context" != "$LOCAL_CLUSTER_CONTEXT" ]; then
    echo "MODE=cluster is limited to local context $LOCAL_CLUSTER_CONTEXT (current: $current_context)." >&2
    echo "The AWS replay path is the relay-deliver-scoped Job rendered by issue #94." >&2
    exit 1
  fi
fi

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

replay() {
  case "$MODE" in
    compose)
      # A one-off relay container inherits the same KAFKA_BOOTSTRAP and
      # KAFKA_AUTH_MODE settings as relay-deliver. The /relay-replay binary is
      # built into the relay image, so this path does not depend on Kafka's
      # Java CLI or a second operational image.
      docker compose -f "$COMPOSE_FILE" run --rm --no-deps \
        --entrypoint /relay-replay "$CONSUMER_SERVICE" "$@"
      ;;
    cluster)
      # This script's cluster mode is the local minikube demo. The live AWS
      # overlay runs this same binary in a short-lived Job with relay-deliver's
      # service account, because only that identity may alter group offsets.
      kubectl -n "$NAMESPACE" exec deploy/relay-ingest -- /relay-replay "$@"
      ;;
  esac
}

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
  date -u -d "${n} ${word} ago" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null \
    || date -u -v"-${n}${bsd}" '+%Y-%m-%dT%H:%M:%SZ'
}

say "stopping $CONSUMER_SERVICE so the group can be reset"
stop_consumer

if [ "$SINCE" = "earliest" ]; then
  say "resetting $GROUP to the beginning of $TOPIC"
  from=earliest
else
  from=$(utc_since "$SINCE")
  say "resetting $GROUP to $from UTC (last $SINCE)"
fi

# The command performs its own authenticated DescribeGroups, Metadata,
# ListOffsets, and OffsetCommit requests. Its output is the replay receipt.
say "waiting for group $GROUP to go inactive, then committing replay offsets"
replay --group "$GROUP" --topic "$TOPIC" --since "$from" --wait 30s \
  | sed 's/^/    /'

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

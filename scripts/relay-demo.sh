#!/usr/bin/env bash
# The M2 demo: six steps, scripted, narrated, hands-off.
#
# You watch a Grafana panel; this drives everything else and explains what it is
# doing and why. The visual sequence remains part of the result: the panel shows
# lag rise and drain, the replica curve, deliveries, and dead letters. The
# script also asserts the first delivery, scaling, dead-letter, and replay
# outcomes from current Prometheus samples. The deeper replay check remains
# `make relay-replay-verify`.
#
# See docs/goal-relay.md#the-demo and
# docs/adr/0008-in-cluster-observability-for-the-demo.md.
#
#   make relay-demo
set -euo pipefail

NAMESPACE="${RELAY_NAMESPACE:-mlp}"
MONITORING_NS="${MONITORING_NS:-monitoring}"
EVENTS="${DEMO_EVENTS:-600}"
TENANTS="${DEMO_TENANTS:-16}"
SLOW_MS="${DEMO_SLOW_MS:-1000}"
GRAFANA_PORT="${DEMO_GRAFANA_PORT:-3001}"
PROM_PORT="${DEMO_PROM_PORT:-9091}"
TOOLBOX_IMAGE="${DEMO_TOOLBOX_IMAGE:-curlimages/curl:8.11.1}"
TOOLBOX=demo-toolbox
PROM_READY_ATTEMPTS="${DEMO_PROM_READY_ATTEMPTS:-120}"
SCALE_POLL_ATTEMPTS="${DEMO_SCALE_POLL_ATTEMPTS:-40}"
SCALE_POLL_SECONDS="${DEMO_SCALE_POLL_SECONDS:-6}"
DELIVERY_POLL_ATTEMPTS="${DEMO_DELIVERY_POLL_ATTEMPTS:-20}"
DLQ_POLL_ATTEMPTS="${DEMO_DLQ_POLL_ATTEMPTS:-20}"
DLQ_POLL_SECONDS="${DEMO_DLQ_POLL_SECONDS:-3}"
METRIC_MAX_AGE_SECONDS="${DEMO_METRIC_MAX_AGE_SECONDS:-30}"

bold=$'\033[1m'; blue=$'\033[1;34m'; dim=$'\033[2m'; red=$'\033[1;31m'; off=$'\033[0m'
step()  { printf '\n%s── %s ──%s\n' "$bold" "$*" "$off"; }
say()   { printf '%s==>%s %s\n' "$blue" "$off" "$*"; }
note()  { printf '%s    %s%s\n' "$dim" "$*" "$off"; }
fail()  { printf '%sFAIL%s %s\n' "$red" "$off" "$*" >&2; exit 1; }

cleanup() {
  if [ -n "${active_pid:-}" ] && kill -0 "$active_pid" 2>/dev/null; then
    kill "$active_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$active_pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$active_pid" 2>/dev/null; then
      kill -KILL "$active_pid" 2>/dev/null || true
    fi
    wait "$active_pid" 2>/dev/null || true
  fi
  if [ -n "${producer:-}" ] && kill -0 "$producer" 2>/dev/null; then
    kill "$producer" 2>/dev/null || true
    wait "$producer" 2>/dev/null || true
  fi
  # `[ -n "$x" ] && kill ... || true` reads as if-then-else and is not one
  # (SC2015). Spelled out, because the port-forwards must be killed even when
  # an earlier line of this trap failed.
  #
  # Note the comment does not begin "# shellcheck" -- that prefix is parsed as a
  # DIRECTIVE, and an unparseable one is an error, not a comment.
  if [ -n "${graf_pid:-}" ]; then kill "$graf_pid" 2>/dev/null || true; fi
  if [ -n "${prom_pid:-}" ]; then kill "$prom_pid" 2>/dev/null || true; fi
  kubectl --request-timeout=15s -n "$NAMESPACE" annotate \
    scaledobject relay-deliver autoscaling.keda.sh/paused-replicas- \
    >/dev/null 2>&1 || true
  # Leaving the sink slow would make the NEXT run look broken from its first
  # step, which is a confusing way to inherit state. Reset it while the toolbox
  # still exists, then remove the toolbox.
  kubectl --request-timeout=15s -n "$NAMESPACE" exec "$TOOLBOX" -- \
    curl --max-time 10 -s -o /dev/null -X POST http://sink:8081/control \
      -d '{"latency_ms":0,"fail_rate":0}' 2>/dev/null || true
  kubectl --request-timeout=15s -n "$NAMESPACE" delete pod "$TOOLBOX" \
    --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# tb runs a command inside the cluster. Every HTTP call the demo makes goes
# through here rather than over `kubectl port-forward`, which does not survive
# a concurrent burst -- it died mid-run the first time this load was generated.
tb() { kubectl --request-timeout=15s -n "$NAMESPACE" exec "$TOOLBOX" -- "$@"; }

promq() {
  local response
  response=$(curl --max-time 5 -sf --get \
    "http://localhost:$PROM_PORT/api/v1/query" \
    --data-urlencode "query=$1" 2>/dev/null) || return 1
  printf '%s' "$response" | python3 -c 'import json,math,sys
try:
    payload = json.load(sys.stdin)
    if payload.get("status") != "success":
        raise ValueError("Prometheus query failed")
    result = payload["data"]["result"]
    if len(result) != 1:
        raise ValueError("Prometheus query returned no unique sample")
    value = float(result[0]["value"][1])
    if not math.isfinite(value) or value < 0:
        raise ValueError("Prometheus query returned an invalid value")
    print(math.ceil(value))
except (KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)' 2>/dev/null
}

read_promq() {
  local destination="$1" query="$2" description="$3" value
  if ! value=$(promq "$query"); then
    fail "Prometheus did not return one valid sample for $description"
  fi
  printf -v "$destination" '%s' "$value"
}

require_fresh_metric() {
  local query="$1" description="$2" age
  read_promq age "$query" "$description freshness"
  [ "$age" -le "$METRIC_MAX_AGE_SECONDS" ] ||
    fail "$description is ${age}s old; maximum accepted age is ${METRIC_MAX_AGE_SECONDS}s"
}

run_interruptibly() {
  local status
  "$@" &
  active_pid=$!
  if wait "$active_pid"; then status=0; else status=$?; fi
  active_pid=
  return "$status"
}

require_nonnegative_integer() {
  local name="$1" value="$2"
  case "$value" in
    '' | *[!0-9]*) fail "$name must be a non-negative integer (got '$value')" ;;
    0 | [1-9]*) ;;
    *) fail "$name must not contain leading zeroes (got '$value')" ;;
  esac
}

require_positive_integer() {
  local name="$1" value="$2"
  require_nonnegative_integer "$name" "$value"
  [ "$value" -gt 0 ] || fail "$name must be greater than zero"
}

scale_succeeded() {
  local released="$1" lag="$2" replicas="$3" peak_lag="$4" max_replicas="$5"
  [ "$released" -eq 1 ] && [ "$lag" -eq 0 ] && [ "$replicas" -eq 1 ] &&
    [ "$peak_lag" -gt 0 ] && [ "$max_replicas" -gt 1 ]
}

dlq_succeeded() {
  local dead_letters_before="$1" dead_letters_after="$2"
  local deliveries_before="$3" deliveries_after="$4"
  [ "$dead_letters_after" -gt "$dead_letters_before" ] &&
    [ "$deliveries_after" -gt "$deliveries_before" ]
}

# The outcome predicates are sourced by their unit tests. Keep every
# state-changing command below this guard.
if [ "${BASH_SOURCE[0]}" != "$0" ]; then
  return 0
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ---------------------------------------------------------------------------
step "step 0 of 6 -- preconditions"
# ---------------------------------------------------------------------------
# Everything here fails the run rather than degrading it. A demo that starts and
# breaks at step 3 wastes the audience's attention; one that refuses in the
# first ten seconds costs nothing.

require_positive_integer DEMO_EVENTS "$EVENTS"
require_positive_integer DEMO_TENANTS "$TENANTS"
require_nonnegative_integer DEMO_SLOW_MS "$SLOW_MS"
require_positive_integer DEMO_GRAFANA_PORT "$GRAFANA_PORT"
require_positive_integer DEMO_PROM_PORT "$PROM_PORT"
require_positive_integer DEMO_PROM_READY_ATTEMPTS "$PROM_READY_ATTEMPTS"
require_positive_integer DEMO_SCALE_POLL_ATTEMPTS "$SCALE_POLL_ATTEMPTS"
require_nonnegative_integer DEMO_SCALE_POLL_SECONDS "$SCALE_POLL_SECONDS"
require_positive_integer DEMO_DELIVERY_POLL_ATTEMPTS "$DELIVERY_POLL_ATTEMPTS"
require_positive_integer DEMO_DLQ_POLL_ATTEMPTS "$DLQ_POLL_ATTEMPTS"
require_nonnegative_integer DEMO_DLQ_POLL_SECONDS "$DLQ_POLL_SECONDS"
require_positive_integer DEMO_METRIC_MAX_AGE_SECONDS "$METRIC_MAX_AGE_SECONDS"
[ "$GRAFANA_PORT" -le 65535 ] || fail "DEMO_GRAFANA_PORT must be at most 65535"
[ "$PROM_PORT" -le 65535 ] || fail "DEMO_PROM_PORT must be at most 65535"

command -v kubectl >/dev/null || fail "kubectl is not on PATH"
kubectl --request-timeout=15s cluster-info >/dev/null 2>&1 ||
  fail "no reachable cluster. Run 'make k8s-up'."

for d in relay-ingest relay-deliver sink; do
  kubectl --request-timeout=15s -n "$NAMESPACE" get deploy "$d" >/dev/null 2>&1 ||
    fail "no Deployment $d in $NAMESPACE. Run 'make k8s-apply-local'."
done

kubectl --request-timeout=15s -n "$NAMESPACE" get scaledobject relay-deliver \
  >/dev/null 2>&1 ||
  fail "no ScaledObject relay-deliver -- KEDA is what steps 3 and 4 demonstrate. Run 'make keda-install'."

# Both consumers in one group split the partitions between them, so half the
# events would be delivered to a sink nobody is looking at. This is the trap
# docs/runbook-k8s.md warns about, and it is invisible once running.
if [ -n "$(docker compose -f local/docker-compose.yml ps -q relay-deliver 2>/dev/null)" ]; then
  fail "$(printf '%s\n' \
    "the compose relay-deliver is running. It joins the SAME consumer group as" \
    "the cluster one and they split the partitions, so half of this demo would" \
    "be delivered where nobody is looking. Stop it:" \
    "  docker compose -f local/docker-compose.yml stop relay-ingest relay-deliver sink")"
fi

# The sink must succeed SLOWLY, not time out. This is documented in
# docs/runbook-k8s.md and it still caught the first run of this script: 2000ms
# against a 2s RELAY_DELIVERY_TIMEOUT meant every delivery timed out, burned the
# whole demo retry budget -- 1s+2s+4s+8s of delays plus four 2s attempts, about
# 23s a record -- and throughput collapsed to 0.45/s while pods scaled to 12.
#
# It reads as KEDA misbehaving. Nothing is wrong with the autoscaling.
# Prose was not enough, so it is a precondition now.
timeout_spec="$(kubectl --request-timeout=15s -n "$NAMESPACE" get cm relay-runtime \
  -o jsonpath='{.data.RELAY_DELIVERY_TIMEOUT}' 2>/dev/null || true)"
case "$timeout_spec" in
  *ms)
    timeout_value="${timeout_spec%ms}"
    require_positive_integer RELAY_DELIVERY_TIMEOUT "$timeout_value"
    timeout_ms="$timeout_value"
    ;;
  *s)
    timeout_value="${timeout_spec%s}"
    require_positive_integer RELAY_DELIVERY_TIMEOUT "$timeout_value"
    timeout_ms=$(( timeout_value * 1000 ))
    ;;
  *)
    fail "RELAY_DELIVERY_TIMEOUT is missing or unsupported (got '$timeout_spec')"
    ;;
esac
if [ "$SLOW_MS" -ge "$timeout_ms" ]; then
  fail "$(printf '%s\n' \
    "DEMO_SLOW_MS=${SLOW_MS} is not below RELAY_DELIVERY_TIMEOUT=${timeout_spec}." \
    "Every delivery would time out and retry rather than succeed slowly, so" \
    "throughput collapses and the demo shows KEDA scaling to 12 pods that drain" \
    "almost nothing. Pick a latency under ${timeout_ms}ms.")"
fi
say "sink latency ${SLOW_MS}ms, under the ${timeout_ms}ms delivery timeout"

say "checking Prometheus is actually scraping the consumers"
run_interruptibly env MONITORING_NAMESPACE="$MONITORING_NS" \
  ./scripts/monitoring-ready.sh >/dev/null ||
  fail "$(printf '%s\n' \
    "Prometheus has no relay-deliver targets, so the panel this demo is read" \
    "off would stay empty. Diagnose with:  make monitoring-ready")"

kubectl --request-timeout=15s -n "$NAMESPACE" delete pod "$TOOLBOX" \
  --ignore-not-found >/dev/null 2>&1
kubectl --request-timeout=15s -n "$NAMESPACE" run "$TOOLBOX" \
  --restart=Never --image="$TOOLBOX_IMAGE" \
  --image-pull-policy=IfNotPresent --command -- sleep 900 >/dev/null
run_interruptibly kubectl -n "$NAMESPACE" wait --for=condition=Ready \
  "pod/$TOOLBOX" --timeout=120s >/dev/null ||
  fail "the in-cluster toolbox pod never became ready"

kubectl -n "$MONITORING_NS" port-forward svc/monitoring-kube-prometheus-prometheus \
  "$PROM_PORT:9090" >/dev/null 2>&1 & prom_pid=$!
kubectl -n "$MONITORING_NS" port-forward svc/monitoring-grafana \
  "$GRAFANA_PORT:80" >/dev/null 2>&1 & graf_pid=$!
prom_ready=0
for _ in $(seq 1 "$PROM_READY_ATTEMPTS"); do
  if curl --max-time 2 -sf -o /dev/null \
    "http://localhost:$PROM_PORT/-/ready" 2>/dev/null; then
    prom_ready=1
    break
  fi
  run_interruptibly sleep 1
done
[ "$prom_ready" -eq 1 ] || fail "Prometheus did not become ready after ${PROM_READY_ATTEMPTS}s"

printf '\n%s    OPEN THIS NOW:  http://localhost:%s/d/relay-delivery%s\n' \
  "$bold" "$GRAFANA_PORT" "$off"
note "no login -- anonymous admin. Everything below is meant to be watched there."
run_interruptibly sleep 8

# ---------------------------------------------------------------------------
step "step 1 of 6 -- one event, delivered immediately"
# ---------------------------------------------------------------------------
say "POST /v1/events with a healthy subscriber"
step1_deliveries_before=0
read_promq step1_deliveries_before \
  'sum(relay_deliveries_total{outcome="delivered"}) or vector(0)' \
  "step 1 delivered counter"
if [ "$step1_deliveries_before" -gt 0 ]; then
  require_fresh_metric \
    'max(time() - timestamp(relay_deliveries_total{outcome="delivered"}))' \
    "step 1 delivered counter"
fi
tb curl --max-time 10 -fsS -o /dev/null -X POST http://sink:8081/control \
  -d '{"latency_ms":0,"fail_rate":0}'
tb curl --max-time 10 -fsS -X POST http://relay-ingest/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo-01","type":"invoice.paid","data":{"amount":100}}'
echo
step1_deliveries="$step1_deliveries_before"
for _ in $(seq 1 "$DELIVERY_POLL_ATTEMPTS"); do
  read_promq step1_deliveries \
    'sum(relay_deliveries_total{outcome="delivered"}) or vector(0)' \
    "step 1 delivered counter"
  [ "$step1_deliveries" -gt "$step1_deliveries_before" ] && break
  run_interruptibly sleep "$DLQ_POLL_SECONDS"
done
[ "$step1_deliveries" -gt "$step1_deliveries_before" ] ||
  fail "step 1 delivery did not appear after $DELIVERY_POLL_ATTEMPTS polls"
require_fresh_metric \
  'max(time() - timestamp(relay_deliveries_total{outcome="delivered"}))' \
  "step 1 delivered counter"
note "ingest wrote it to Kafka keyed by tenant, the consumer group picked it up,"
note "and it was POSTed to the subscriber signed per Standard Webhooks."

# ---------------------------------------------------------------------------
step "step 2 of 6 -- slow the subscriber, open the tap"
# ---------------------------------------------------------------------------
say "setting the sink to answer in ${SLOW_MS}ms"
tb curl --max-time 10 -fsS -X POST http://sink:8081/control \
  -d "{\"latency_ms\":${SLOW_MS}}"; echo
note "a subscriber that succeeds SLOWLY, not one that fails. Backpressure, not errors."
note "(above RELAY_DELIVERY_TIMEOUT the attempts would time out and retry instead,"
note " which collapses throughput and looks like KEDA misbehaving -- runbook-k8s.md)"

say "producing $EVENTS events across $TENANTS tenants"
note "many tenants on purpose: the partition key is the tenant id, so one tenant's"
note "events all land on ONE partition and exactly one consumer can ever work on them."
kubectl -n "$NAMESPACE" exec "$TOOLBOX" -- sh -c "
set -eu
failed=0
pids=''
for i in \$(seq 1 $EVENTS); do
  t=\$(printf 'demo-%02d' \$(( i % $TENANTS + 1 )))
  curl --max-time 10 -fsS -o /dev/null -X POST http://relay-ingest/v1/events \
    -H 'Content-Type: application/json' \
    -d \"{\\\"tenant_id\\\":\\\"\$t\\\",\\\"type\\\":\\\"invoice.paid\\\",\\\"data\\\":{\\\"n\\\":\$i}}\" &
  pids=\"\$pids \$!\"
  if [ \$(( i % 30 )) -eq 0 ]; then
    for pid in \$pids; do wait \"\$pid\" || failed=1; done
    [ \"\$failed\" -eq 0 ] || exit 1
    pids=''
  fi
done
for pid in \$pids; do wait \"\$pid\" || failed=1; done
[ \"\$failed\" -eq 0 ]" >/dev/null 2>&1 &
producer=$!

# ---------------------------------------------------------------------------
step "steps 3 and 4 of 6 -- KEDA scales on lag, then scales back"
# ---------------------------------------------------------------------------
note "lag is read from the broker by relay-ingest, which is where KEDA reads it too,"
note "so the panel and the scaler cannot disagree. max() not sum(): every ingest"
note "replica publishes the same number."
echo
printf '    %-8s %-8s %s\n' "t" "lag" "consumers"
start=$(date +%s)
released=0
saw_backlog=0
saw_falling=0
previous_lag=-1
peak_lag=0
max_reps=0
lag=0
reps=0
backlog_threshold=$(( EVENTS / 4 ))
[ "$backlog_threshold" -gt 0 ] || backlog_threshold=1
for _ in $(seq 1 "$SCALE_POLL_ATTEMPTS"); do
  t=$(( $(date +%s) - start ))
  read_promq lag 'max(relay_consumer_group_lag_total{group="relay-deliver"})' \
    "relay consumer lag"
  read_promq reps 'count(relay_build_info{role="deliver"})' \
    "relay delivery consumer count"
  if [ "$lag" -gt "$peak_lag" ]; then peak_lag="$lag"; fi
  if [ "$reps" -gt "$max_reps" ]; then max_reps="$reps"; fi
  if [ "$lag" -ge "$backlog_threshold" ]; then saw_backlog=1; fi
  if [ "$previous_lag" -ge 0 ] && [ "$lag" -lt "$previous_lag" ]; then
    saw_falling=1
  fi
  printf '    t=%-6s %-8s %s\n' "${t}s" "$lag" "$reps"

  # Step 4 begins the moment the backlog is clearly draining, rather than at a
  # fixed time -- a slow machine would otherwise release the sink mid-climb.
  if [ "$released" -eq 0 ] && [ "$saw_backlog" -eq 1 ] && \
    [ "$saw_falling" -eq 1 ] && [ "$max_reps" -gt 1 ] && \
    [ "$lag" -lt "$backlog_threshold" ] && [ "$t" -gt 30 ]; then
    echo
    say "releasing the sink -- lag is draining, so scale-down is next"
    tb curl --max-time 10 -fsS -o /dev/null -X POST \
      http://sink:8081/control -d '{"latency_ms":0}'
    released=1
    echo
  fi
  if [ "$released" -eq 1 ] && [ "$lag" -eq 0 ] && [ "$reps" -eq 1 ]; then
    require_fresh_metric \
      'time() - min(relay_lag_refreshed_timestamp_seconds and on(instance) relay_build_info{role="ingest"})' \
      "relay broker lag measurement"
    require_fresh_metric \
      'max(time() - timestamp(relay_build_info{role="deliver"}))' \
      "relay delivery consumer count"
    break
  fi
  # A backlog that is not moving with consumers at the ceiling is the failure
  # above, or a subscriber that is down. Say which rather than looping quietly.
  if [ "$t" -gt 90 ] && [ "$reps" -ge 10 ] && [ "$lag" -gt $(( EVENTS * 3 / 4 )) ]; then
    echo
    note "lag is barely moving with $reps consumers running. That is throughput"
    note "collapse, not slow scaling -- check attempts by status class on the panel."
  fi
  previous_lag="$lag"
  run_interruptibly sleep "$SCALE_POLL_SECONDS"
done
if ! wait "$producer"; then
  producer=
  fail "the event producer failed before sending all $EVENTS events"
fi
producer=
scale_succeeded "$released" "$lag" "$reps" "$peak_lag" "$max_reps" ||
  fail "scaling did not complete after $SCALE_POLL_ATTEMPTS polls (peak lag=$peak_lag, max consumers=$max_reps, final lag=$lag, final consumers=$reps, sink released=$released)"
note "twelve partitions is the ceiling on useful consumers: a thirteenth pod would"
note "be assigned nothing and drain nothing."

# ---------------------------------------------------------------------------
step "step 5 of 6 -- one failing subscriber does not block a healthy one"
# ---------------------------------------------------------------------------
say "tenant 'acme' is seeded with two subscribers: /hooks/ok and /hooks/flaky"
note "/hooks/flaky always answers 500. The retry preset in-cluster is 1s+2s+4s+8s,"
note "so the dead-letter takes about 15 seconds -- watchable, which is the point."
dl_before=0
ok_before=0
read_promq dl_before 'sum(relay_dead_letters_total) or vector(0)' \
  "dead-letter counter baseline"
read_promq ok_before \
  'sum(relay_deliveries_total{outcome="delivered"}) or vector(0)' \
  "delivered counter baseline"
if [ "$dl_before" -gt 0 ]; then
  require_fresh_metric 'max(time() - timestamp(relay_dead_letters_total))' \
    "dead-letter counter"
fi
if [ "$ok_before" -gt 0 ]; then
  require_fresh_metric \
    'max(time() - timestamp(relay_deliveries_total{outcome="delivered"}))' \
    "delivered counter"
fi
tb curl --max-time 10 -fsS -X POST http://relay-ingest/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"acme","type":"invoice.paid","data":{"amount":250}}'
echo
dl="$dl_before"
ok="$ok_before"
for _ in $(seq 1 "$DLQ_POLL_ATTEMPTS"); do
  read_promq dl 'sum(relay_dead_letters_total) or vector(0)' "dead-letter counter"
  read_promq ok \
    'sum(relay_deliveries_total{outcome="delivered"}) or vector(0)' \
    "delivered counter"
  printf '    delivered=%-6s dead-lettered=%s\n' "$ok" "$dl"
  dlq_succeeded "$dl_before" "$dl" "$ok_before" "$ok" && break
  run_interruptibly sleep "$DLQ_POLL_SECONDS"
done
dlq_succeeded "$dl_before" "$dl" "$ok_before" "$ok" ||
  fail "subscriber outcomes did not complete after $DLQ_POLL_ATTEMPTS polls (delivered $ok_before->$ok, dead-lettered $dl_before->$dl)"
require_fresh_metric 'max(time() - timestamp(relay_dead_letters_total))' \
  "dead-letter counter"
require_fresh_metric \
  'max(time() - timestamp(relay_deliveries_total{outcome="delivered"}))' \
  "delivered counter"
note "the healthy subscriber was delivered to immediately; the failing one exhausted"
note "its budget and went to the DLQ with a reason. Concurrent per subscriber, so"
note "one does not wait out the other's retries."

# ---------------------------------------------------------------------------
step "step 6 of 6 -- replay, the thing a queue cannot do"
# ---------------------------------------------------------------------------
note "the log still holds every event, so redelivery is an offset reset rather than"
note "a feature relay had to build. A queue deletes on acknowledgement -- this step"
note "is the argument in ADR 0006, executed."
echo
replay_deliveries_before=0
read_promq replay_deliveries_before \
  'sum(relay_deliveries_total{outcome="delivered"}) or vector(0)' \
  "replay delivered counter"
run_interruptibly env MODE=cluster SINCE=10m ./scripts/relay-replay.sh
replay_deliveries="$replay_deliveries_before"
for _ in $(seq 1 "$DELIVERY_POLL_ATTEMPTS"); do
  read_promq replay_deliveries \
    'sum(relay_deliveries_total{outcome="delivered"}) or vector(0)' \
    "replay delivered counter"
  [ "$replay_deliveries" -gt "$replay_deliveries_before" ] && break
  run_interruptibly sleep "$DLQ_POLL_SECONDS"
done
[ "$replay_deliveries" -gt "$replay_deliveries_before" ] ||
  fail "replayed deliveries did not appear after $DELIVERY_POLL_ATTEMPTS polls"
require_fresh_metric \
  'max(time() - timestamp(relay_deliveries_total{outcome="delivered"}))' \
  "replay delivered counter"

say "resetting the sink controls"
tb curl --max-time 10 -fsS -o /dev/null -X POST http://sink:8081/control \
  -d '{"latency_ms":0,"fail_rate":0}' || fail "could not reset the sink controls"

echo
step "done"
note "panel: http://localhost:$GRAFANA_PORT/d/relay-delivery"
note "the sink has been reset to answer immediately."

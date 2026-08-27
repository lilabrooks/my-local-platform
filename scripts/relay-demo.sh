#!/usr/bin/env bash
# The M2 demo: six steps, scripted, narrated, hands-off.
#
# You watch a Grafana panel; this drives everything else and explains what it is
# doing and why. It asserts its own PRECONDITIONS but not its outcomes -- what
# the six steps demonstrate is read off the panel by a person. A demo that
# judged its own success would have to encode what "lag drained" means, and the
# argument here is visual. The self-checking path is `make relay-replay-verify`.
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

bold=$'\033[1m'; blue=$'\033[1;34m'; dim=$'\033[2m'; red=$'\033[1;31m'; off=$'\033[0m'
step()  { printf '\n%s── %s ──%s\n' "$bold" "$*" "$off"; }
say()   { printf '%s==>%s %s\n' "$blue" "$off" "$*"; }
note()  { printf '%s    %s%s\n' "$dim" "$*" "$off"; }
fail()  { printf '%sFAIL%s %s\n' "$red" "$off" "$*" >&2; exit 1; }

cleanup() {
  kubectl -n "$NAMESPACE" delete pod "$TOOLBOX" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  # `[ -n "$x" ] && kill ... || true` reads as if-then-else and is not one
  # (SC2015). Spelled out, because the port-forwards must be killed even when
  # an earlier line of this trap failed.
  #
  # Note the comment does not begin "# shellcheck" -- that prefix is parsed as a
  # DIRECTIVE, and an unparseable one is an error, not a comment.
  if [ -n "${graf_pid:-}" ]; then kill "$graf_pid" 2>/dev/null || true; fi
  if [ -n "${prom_pid:-}" ]; then kill "$prom_pid" 2>/dev/null || true; fi
  # Leaving the sink slow would make the NEXT run look broken from its first
  # step, which is a confusing way to inherit state.
  tb curl -s -o /dev/null -X POST http://sink:8081/control -d '{"latency_ms":0,"fail_rate":0}' 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# tb runs a command inside the cluster. Every HTTP call the demo makes goes
# through here rather than over `kubectl port-forward`, which does not survive
# a concurrent burst -- it died mid-run the first time this load was generated.
tb() { kubectl -n "$NAMESPACE" exec "$TOOLBOX" -- "$@"; }

promq() {
  curl -s --get "http://localhost:$PROM_PORT/api/v1/query" --data-urlencode "query=$1" 2>/dev/null |
    python3 -c 'import json,sys
try:
    r = json.load(sys.stdin)["data"]["result"]
    print(int(float(r[0]["value"][1])) if r else 0)
except Exception:
    print(0)' 2>/dev/null || echo 0
}

# ---------------------------------------------------------------------------
step "step 0 of 6 -- preconditions"
# ---------------------------------------------------------------------------
# Everything here fails the run rather than degrading it. A demo that starts and
# breaks at step 3 wastes the audience's attention; one that refuses in the
# first ten seconds costs nothing.

command -v kubectl >/dev/null || fail "kubectl is not on PATH"
kubectl cluster-info >/dev/null 2>&1 || fail "no reachable cluster. Run 'make k8s-up'."

for d in relay-ingest relay-deliver sink; do
  kubectl -n "$NAMESPACE" get deploy "$d" >/dev/null 2>&1 ||
    fail "no Deployment $d in $NAMESPACE. Run 'make k8s-apply-local'."
done

kubectl -n "$NAMESPACE" get scaledobject relay-deliver >/dev/null 2>&1 ||
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
timeout_spec="$(kubectl -n "$NAMESPACE" get cm relay -o jsonpath='{.data.RELAY_DELIVERY_TIMEOUT}' 2>/dev/null || true)"
case "$timeout_spec" in
  *ms) timeout_ms="${timeout_spec%ms}" ;;
  *s)  timeout_ms=$(( ${timeout_spec%s} * 1000 )) ;;
  *)   timeout_ms=0 ;;
esac
if [ "$timeout_ms" -gt 0 ] && [ "$SLOW_MS" -ge "$timeout_ms" ]; then
  fail "$(printf '%s\n' \
    "DEMO_SLOW_MS=${SLOW_MS} is not below RELAY_DELIVERY_TIMEOUT=${timeout_spec}." \
    "Every delivery would time out and retry rather than succeed slowly, so" \
    "throughput collapses and the demo shows KEDA scaling to 12 pods that drain" \
    "almost nothing. Pick a latency under ${timeout_ms}ms.")"
fi
say "sink latency ${SLOW_MS}ms, under the ${timeout_ms}ms delivery timeout"

say "checking Prometheus is actually scraping the consumers"
MONITORING_NAMESPACE="$MONITORING_NS" ./scripts/monitoring-ready.sh >/dev/null ||
  fail "$(printf '%s\n' \
    "Prometheus has no relay-deliver targets, so the panel this demo is read" \
    "off would stay empty. Diagnose with:  make monitoring-ready")"

kubectl -n "$NAMESPACE" delete pod "$TOOLBOX" --ignore-not-found >/dev/null 2>&1
kubectl -n "$NAMESPACE" run "$TOOLBOX" --restart=Never --image="$TOOLBOX_IMAGE" \
  --image-pull-policy=IfNotPresent --command -- sleep 900 >/dev/null
kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$TOOLBOX" --timeout=120s >/dev/null ||
  fail "the in-cluster toolbox pod never became ready"

kubectl -n "$MONITORING_NS" port-forward svc/monitoring-kube-prometheus-prometheus \
  "$PROM_PORT:9090" >/dev/null 2>&1 & prom_pid=$!
kubectl -n "$MONITORING_NS" port-forward svc/monitoring-grafana \
  "$GRAFANA_PORT:80" >/dev/null 2>&1 & graf_pid=$!
until curl -sf -o /dev/null "http://localhost:$PROM_PORT/-/ready" 2>/dev/null; do sleep 1; done

printf '\n%s    OPEN THIS NOW:  http://localhost:%s/d/relay-delivery%s\n' \
  "$bold" "$GRAFANA_PORT" "$off"
note "no login -- anonymous admin. Everything below is meant to be watched there."
sleep 8

# ---------------------------------------------------------------------------
step "step 1 of 6 -- one event, delivered immediately"
# ---------------------------------------------------------------------------
say "POST /v1/events with a healthy subscriber"
tb curl -s -o /dev/null -X POST http://sink:8081/control -d '{"latency_ms":0,"fail_rate":0}'
tb curl -s -X POST http://relay-ingest/v1/events -H 'Content-Type: application/json' \
  -d '{"tenant_id":"demo-01","type":"invoice.paid","data":{"amount":100}}'
echo
sleep 3
note "ingest wrote it to Kafka keyed by tenant, the consumer group picked it up,"
note "and it was POSTed to the subscriber signed per Standard Webhooks."

# ---------------------------------------------------------------------------
step "step 2 of 6 -- slow the subscriber, open the tap"
# ---------------------------------------------------------------------------
say "setting the sink to answer in ${SLOW_MS}ms"
tb curl -s -X POST http://sink:8081/control -d "{\"latency_ms\":${SLOW_MS}}"; echo
note "a subscriber that succeeds SLOWLY, not one that fails. Backpressure, not errors."
note "(above RELAY_DELIVERY_TIMEOUT the attempts would time out and retry instead,"
note " which collapses throughput and looks like KEDA misbehaving -- runbook-k8s.md)"

say "producing $EVENTS events across $TENANTS tenants"
note "many tenants on purpose: the partition key is the tenant id, so one tenant's"
note "events all land on ONE partition and exactly one consumer can ever work on them."
kubectl -n "$NAMESPACE" exec "$TOOLBOX" -- sh -c "
for i in \$(seq 1 $EVENTS); do
  t=\$(printf 'demo-%02d' \$(( i % $TENANTS + 1 )))
  curl -s -o /dev/null -X POST http://relay-ingest/v1/events \
    -H 'Content-Type: application/json' \
    -d \"{\\\"tenant_id\\\":\\\"\$t\\\",\\\"type\\\":\\\"invoice.paid\\\",\\\"data\\\":{\\\"n\\\":\$i}}\" &
  [ \$(( i % 30 )) -eq 0 ] && wait
done
wait" >/dev/null 2>&1 &
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
for _ in $(seq 1 40); do
  t=$(( $(date +%s) - start ))
  lag=$(promq 'max(relay_consumer_group_lag_total{group="relay-deliver"})')
  reps=$(promq 'count(relay_build_info{role="deliver"})')
  printf '    t=%-6s %-8s %s\n' "${t}s" "$lag" "$reps"

  # Step 4 begins the moment the backlog is clearly draining, rather than at a
  # fixed time -- a slow machine would otherwise release the sink mid-climb.
  if [ "$released" -eq 0 ] && [ "$lag" -lt 150 ] && [ "$t" -gt 30 ]; then
    echo
    say "releasing the sink -- lag is draining, so scale-down is next"
    tb curl -s -o /dev/null -X POST http://sink:8081/control -d '{"latency_ms":0}'
    released=1
    echo
  fi
  [ "$released" -eq 1 ] && [ "$lag" -eq 0 ] && [ "$reps" -le 1 ] && break
  # A backlog that is not moving with consumers at the ceiling is the failure
  # above, or a subscriber that is down. Say which rather than looping quietly.
  if [ "$t" -gt 90 ] && [ "$reps" -ge 10 ] && [ "$lag" -gt $(( EVENTS * 3 / 4 )) ]; then
    echo
    note "lag is barely moving with $reps consumers running. That is throughput"
    note "collapse, not slow scaling -- check attempts by status class on the panel."
  fi
  sleep 6
done
wait "$producer" 2>/dev/null || true
note "twelve partitions is the ceiling on useful consumers: a thirteenth pod would"
note "be assigned nothing and drain nothing."

# ---------------------------------------------------------------------------
step "step 5 of 6 -- one failing subscriber does not block a healthy one"
# ---------------------------------------------------------------------------
say "tenant 'acme' is seeded with two subscribers: /hooks/ok and /hooks/flaky"
note "/hooks/flaky always answers 500. The retry preset in-cluster is 1s+2s+4s+8s,"
note "so the dead-letter takes about 15 seconds -- watchable, which is the point."
tb curl -s -X POST http://relay-ingest/v1/events -H 'Content-Type: application/json' \
  -d '{"tenant_id":"acme","type":"invoice.paid","data":{"amount":250}}'
echo
for _ in $(seq 1 12); do
  dl=$(promq 'sum(relay_dead_letters_total)')
  ok=$(promq 'sum(relay_deliveries_total{outcome="delivered"})')
  printf '    delivered=%-6s dead-lettered=%s\n' "$ok" "$dl"
  [ "$dl" -gt 0 ] && break
  sleep 3
done
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
MODE=cluster SINCE=10m ./scripts/relay-replay.sh

echo
step "done"
note "panel: http://localhost:$GRAFANA_PORT/d/relay-delivery"
note "the sink has been reset to answer immediately."

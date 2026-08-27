#!/usr/bin/env bash
# Assert the demo's central panel will actually have data, before the demo runs.
#
# `make relay-demo` opens with "lag climbs on a Grafana panel". At least five
# separate faults leave that panel empty, and every one of them looks identical
# from the outside:
#
#   - the ServiceMonitor is missing the `release` label the chart selects on
#   - its port name or Service selector does not match
#   - ArgoCD has not synced it yet
#   - kube-prometheus-stack is not installed at all
#   - relay-deliver is not running
#
# Checking that the pieces EXIST catches only the fourth. So this asserts the
# query the panel actually plots, which covers all five at once:
#
#   count(relay_build_info{role="deliver"}) >= 1
#
# Reasoning recorded in docs/adr/0008-in-cluster-observability-for-the-demo.md,
# under "The label stays, and the guard is a runtime assertion".
#
#   ./scripts/monitoring-ready.sh          # or: make monitoring-ready
set -euo pipefail

NAMESPACE="${MONITORING_NAMESPACE:-monitoring}"
RELEASE="${MONITORING_RELEASE:-monitoring}"
# A free port. 3000 is the compose Grafana and 9090 the compose Prometheus;
# binding either would silently query the WRONG stack, which is a worse failure
# than not being able to bind at all.
PORT="${MONITORING_PROM_PORT:-9091}"
TIMEOUT="${MONITORING_READY_TIMEOUT:-90}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

command -v kubectl >/dev/null || fail "kubectl is not on PATH"

kubectl cluster-info >/dev/null 2>&1 || fail \
  "no reachable cluster. Run 'make k8s-up' first."

# Named separately from the query below only because "the CRD is absent" has a
# different fix -- `make monitoring-install` -- than "the query returned
# nothing", and saying so beats a bare timeout.
if ! kubectl get crd servicemonitors.monitoring.coreos.com >/dev/null 2>&1; then
  fail "the prometheus-operator CRDs are absent. Run 'make monitoring-install'."
fi

# Guessed wrong the first time this was run: the chart's fullname template does
# not truncate the way its docs' examples suggest. Discovered rather than
# hardcoded, so a release name of any length still resolves -- and so this
# reports "not installed" only when it genuinely is not.
prom_svc="$(kubectl -n "$NAMESPACE" get svc \
  -l app=kube-prometheus-stack-prometheus \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [ -z "$prom_svc" ]; then
  prom_svc="$RELEASE-kube-prometheus-prometheus"
fi
if ! kubectl -n "$NAMESPACE" get svc "$prom_svc" >/dev/null 2>&1; then
  fail "no Service $prom_svc in namespace $NAMESPACE. Run 'make monitoring-install'."
fi

say "port-forwarding $prom_svc to localhost:$PORT"
kubectl -n "$NAMESPACE" port-forward "svc/$prom_svc" "$PORT:9090" >/dev/null 2>&1 &
pf_pid=$!
# Always tear the tunnel down, including on failure -- a leaked port-forward
# holds the port and makes the NEXT run fail for an unrelated reason.
trap 'kill "$pf_pid" 2>/dev/null || true' EXIT

query() {
  curl -sf --get "http://localhost:$PORT/api/v1/query" \
    --data-urlencode "query=$1" 2>/dev/null || true
}

say "waiting for consumer metrics (up to ${TIMEOUT}s)"
deadline=$(( $(date +%s) + TIMEOUT ))
consumers=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  response="$(query 'count(relay_build_info{role="deliver"})')"
  consumers="$(printf '%s' "$response" |
    python3 -c 'import json,sys
try:
    r = json.load(sys.stdin)
    print(int(float(r["data"]["result"][0]["value"][1])) if r["data"]["result"] else 0)
except Exception:
    print(0)' 2>/dev/null || echo 0)"
  [ "$consumers" -ge 1 ] && break
  sleep 3
done

if [ "$consumers" -lt 1 ]; then
  printf '\n' >&2
  fail "$(cat <<EOM
Prometheus has no relay-deliver targets, so the demo's lag panel would be empty.

  count(relay_build_info{role="deliver"})  returned nothing after ${TIMEOUT}s

The pieces can all be present and this still fail. In likely order:

  1. The ServiceMonitor is not selected. The chart's Prometheus requires
     'release: $RELEASE' on it, and drops one without that label silently:
       kubectl -n mlp get servicemonitor mlp-services -o jsonpath='{.metadata.labels}'
  2. ArgoCD has not synced it:
       kubectl -n mlp get servicemonitor
  3. relay-deliver is not running:
       kubectl -n mlp get pods -l app.kubernetes.io/name=relay-deliver
  4. The targets are known but failing to scrape:
       open http://localhost:$PORT/targets while a port-forward is up
EOM
)"
fi

say "OK -- $consumers relay-deliver target(s) scraped; the lag panel has data"

#!/usr/bin/env bash
# Prove SIGTERM drains the record already in hand before the consumer exits.
# The healthy subscriber must see the event once, the process must exit cleanly,
# and a same-partition probe after restart must pass the committed offset without
# redelivering the original event.
#
#   make relay-verify-graceful-drain
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

COMPOSE=(docker compose -f local/docker-compose.yml)
INGEST="${RELAY_INGEST_URL:-http://localhost:8082}"
DELIVER="${RELAY_DELIVER_URL:-http://localhost:8083}"
SINK="${SINK_URL:-http://localhost:8084}"
GROUP="${RELAY_CONSUMER_GROUP:-relay-deliver}"
TENANT="${TENANT:-acme}"
BLOCKED_PATH="${BLOCKED_PATH:-/hooks/flaky}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }
note() { printf '\033[2m    %s\033[0m\n' "$*"; }

MARKER="drain-$(date +%s)-$$"
PROBE_MARKER="$MARKER-probe"
CTR=""

cleanup() {
  curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"release\":\"$BLOCKED_PATH\"}" 2>/dev/null || true
  if [ -n "$CTR" ] && [ "$(docker inspect -f '{{.State.Running}}' "$CTR" 2>/dev/null || true)" = "false" ]; then
    docker start "$CTR" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

deliver_container() { "${COMPOSE[@]}" ps -a -q relay-deliver | head -1; }
kafka() { local tool="$1"; shift; docker exec mlp-kafka "/opt/kafka/bin/$tool" "$@"; }
members_raw() {
  kafka kafka-consumer-groups.sh --bootstrap-server localhost:19092 \
    --group "$GROUP" --describe --members --verbose 2>/dev/null | awk 'NF>0 && $1!="GROUP"'
}

delivered_count() {
  local marker="$1"
  curl -sf "$SINK/received" \
    | MARKER="$marker" python3 -c 'import json,os,sys
marker = os.environ["MARKER"]
d = json.load(sys.stdin)
count = 0
for x in (d.get("deliveries") or []):
    if x.get("path") != "/hooks/ok" or not (200 <= x.get("status", 0) < 300):
        continue
    data = x.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError:
            continue
    if isinstance(data, dict) and data.get("marker") == marker:
        count += 1
print(count)'
}

post() {
  local marker="$1" code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGEST/v1/events" \
    -H 'content-type: application/json' \
    -d "{\"tenant_id\":\"$TENANT\",\"type\":\"drain.check\",\"data\":{\"marker\":\"$marker\"}}")
  [ "$code" = "202" ] || fail "ingest returned $code, expected 202"
}

say "checking relay and the sink are up"
curl -sf "$INGEST/readyz" >/dev/null || fail "relay ingest is not reachable at $INGEST -- run 'make up-apps'"
curl -sf "$SINK/healthz" >/dev/null || fail "the sink is not reachable at $SINK -- run 'make up-apps'"

CTR=$(deliver_container)
[ -n "$CTR" ] || fail "no relay-deliver container exists -- run 'make up-apps'"
[ "$(docker inspect -f '{{.State.Running}}' "$CTR")" = "true" ] || fail "relay-deliver is not running"

say "checking consumer group $GROUP has exactly one member"
for _ in $(seq 1 30); do
  member_count=$(members_raw | wc -l | tr -d ' ')
  [ "$member_count" = "1" ] && break
  sleep 1
done
[ "${member_count:-0}" = "1" ] || fail "consumer group $GROUP has ${member_count:-0} members, want exactly 1"

say "clearing the sink and latching $BLOCKED_PATH"
curl -sf -o /dev/null -X DELETE "$SINK/received" || fail "could not clear $SINK/received"
curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"latch\":\"$BLOCKED_PATH\"}" ||
  fail "could not latch $BLOCKED_PATH"

say "posting one event for tenant $TENANT"
post "$MARKER"

say "waiting until the healthy delivery is recorded and the failing one is parked"
for _ in $(seq 1 1200); do
  before=$(delivered_count "$MARKER")
  held=$(curl -sf "$SINK/control" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("held",{}).get("'"$BLOCKED_PATH"'",0))' 2>/dev/null || echo 0)
  [ "$before" = "1" ] && [ "$held" -ge 1 ] && break
  sleep 0.05
done
[ "${before:-0}" = "1" ] || fail "expected exactly one healthy delivery before SIGTERM, found ${before:-0}"
[ "${held:-0}" -ge 1 ] || fail "nothing parked on $BLOCKED_PATH; there is no in-flight record to drain"

say "sending SIGTERM and allowing the configured 45s container stop timeout"
started=$(date +%s)
docker stop --time 45 "$CTR" >/dev/null || fail "docker stop failed"
elapsed=$(($(date +%s) - started))
exit_code=$(docker inspect -f '{{.State.ExitCode}}' "$CTR")
[ "$exit_code" = "0" ] || fail "relay-deliver exited $exit_code, want 0; the drain did not finish cleanly"
[ "$(delivered_count "$MARKER")" = "1" ] || fail "the healthy delivery count changed during graceful shutdown"
note "relay-deliver exited cleanly after ${elapsed}s with one healthy delivery"

say "releasing the latch and restarting the consumer"
curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"release\":\"$BLOCKED_PATH\"}" ||
  fail "could not release $BLOCKED_PATH"
docker start "$CTR" >/dev/null || fail "could not restart relay-deliver"

for _ in $(seq 1 60); do
  curl -sf "$DELIVER/readyz" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "$DELIVER/readyz" >/dev/null || fail "relay-deliver did not become ready after restart"

# The Kafka key is tenant_id, so this probe follows the original record on the
# same partition. Seeing it proves the restarted serial consumer advanced past
# the original offset. Any uncommitted original would be delivered first.
say "posting a same-tenant probe after restart"
post "$PROBE_MARKER"
for _ in $(seq 1 900); do
  [ "$(delivered_count "$PROBE_MARKER")" -ge 1 ] && break
  sleep 0.1
done
[ "$(delivered_count "$PROBE_MARKER")" -eq 1 ] || fail "the post-restart probe was not delivered exactly once"

after=$(delivered_count "$MARKER")
[ "$after" = "1" ] || fail "the original healthy delivery grew from 1 to $after after restart; its offset was not committed"

pass "SIGTERM drained and committed the in-flight record without duplicating it (1 -> $after)"

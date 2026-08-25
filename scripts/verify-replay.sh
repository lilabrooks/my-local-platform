#!/usr/bin/env bash
# Prove that replay actually redelivers.
#
# ADR 0006 chose Kafka over SNS-to-SQS on one deciding technical argument: a
# queue deletes a message on acknowledgement, so it cannot resend what it
# already delivered, while a log still has it. That argument went unexercised
# through the whole of M1 -- this is the check that stops it being a claim.
#
# Post events, watch them arrive, wipe the record of them, move the consumer
# group back in time, and assert the same event ids arrive again.
set -euo pipefail

INGEST="${RELAY_INGEST_URL:-http://localhost:8082}"
SINK="${SINK_URL:-http://localhost:8084}"
EVENTS="${EVENTS:-3}"
TENANT="${TENANT:-acme}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }

# Ids delivered successfully to the healthy endpoint. The flaky subscriber is
# expected to fail and dead-letter; it is not what this asserts.
delivered_ids() {
  curl -sf "$SINK/received" \
    | python3 -c 'import json,sys
d = json.load(sys.stdin)
for x in (d.get("deliveries") or []):
    if x.get("path") == "/hooks/ok" and 200 <= x.get("status", 0) < 300:
        print(x["webhook_id"])'
}

# await_count <count> <seconds> -- for the first pass, when no ids are known yet.
await_count() {
  local want="$1" budget="$2" waited=0
  while :; do
    local n
    n=$(delivered_ids | sort -u | wc -l | tr -d ' ')
    [ "$n" -ge "$want" ] && return 0
    waited=$((waited + 1))
    [ "$waited" -ge "$budget" ] && return 1
    sleep 1
  done
}

# await_these <seconds> -- waits for every id on stdin to be delivered.
#
# Counting is not enough here. A replay redelivers everything in the window,
# including events from earlier runs, so waiting for "three ids" can finish on
# three older ones while the ids under test are still coming. That produced a
# false failure until this waited for the specific ids instead.
await_these() {
  local budget="$1" waited=0 wanted
  wanted=$(cat)
  while :; do
    local missing
    missing=$(comm -23 <(echo "$wanted") <(delivered_ids | sort -u) || true)
    [ -z "$missing" ] && return 0
    waited=$((waited + 1))
    [ "$waited" -ge "$budget" ] && return 1
    sleep 1
  done
}

say "checking relay and the sink are up"
curl -sf "$INGEST/readyz"  >/dev/null || fail "relay ingest is not reachable at $INGEST -- run 'make up-apps'"
curl -sf "$SINK/healthz"   >/dev/null || fail "the sink is not reachable at $SINK -- run 'make up-apps'"

# The window has to start before the events exist. A second of slack absorbs
# any clock skew between this shell and the broker.
sleep 1
started_at=$(date -u '+%s')

say "clearing the sink"
curl -sf -X DELETE "$SINK/received" >/dev/null

say "posting $EVENTS events as tenant $TENANT"
for i in $(seq 1 "$EVENTS"); do
  curl -sf -X POST "$INGEST/v1/events" -H 'Content-Type: application/json' \
    -d "{\"tenant_id\":\"$TENANT\",\"type\":\"replay.check\",\"data\":{\"n\":$i}}" >/dev/null \
    || fail "ingest rejected event $i"
done

say "waiting for the first delivery of all $EVENTS"
await_count "$EVENTS" 60 || fail "only $(delivered_ids | sort -u | wc -l | tr -d ' ') of $EVENTS were delivered before replay"
before=$(delivered_ids | sort -u)
pass "delivered $(echo "$before" | wc -l | tr -d ' ') events"

# Wiping the sink is what makes the assertion mean something: anything seen
# afterwards has genuinely been sent a second time, not remembered.
say "clearing the sink so a redelivery cannot be confused with the first one"
curl -sf -X DELETE "$SINK/received" >/dev/null
remaining=$(delivered_ids | wc -l | tr -d ' ')
[ "$remaining" = "0" ] || fail "sink still reports $remaining deliveries after being cleared"

# Round the window down to whole minutes and go back one more, because
# --to-datetime resolves to the first offset at or after the timestamp.
minutes_ago=$(( ($(date -u '+%s') - started_at) / 60 + 2 ))
say "replaying the last ${minutes_ago}m"
SINCE="${minutes_ago}m" ./scripts/relay-replay.sh

say "waiting for those exact events to arrive again"
if ! echo "$before" | await_these 90; then
  after=$(delivered_ids | sort -u)
  fail "these events were delivered once but not redelivered:
$(comm -23 <(echo "$before") <(echo "$after") || true)"
fi
after=$(delivered_ids | sort -u)

pass "every event was delivered again after an offset reset"
echo
echo "  events posted and redelivered: $EVENTS"
echo "  total delivered in the window:  $(echo "$after" | wc -l | tr -d ' ')"
echo
echo "  The window covers earlier events too, which is the point -- a replay"
echo "  resends everything after the timestamp, not just what was asked for."
echo
echo "  A queue could not do this. The messages were acknowledged and would"
echo "  have been deleted; the log still had them. See ADR 0006."

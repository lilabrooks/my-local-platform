#!/usr/bin/env bash
# Prove that a consumer dying between delivery and commit produces a DUPLICATE,
# not a lost event -- and that the duplicate carries the same webhook-id, which
# is the only reason at-least-once is a usable contract.
#
# ADR 0006 has listed this under "Still planned" since the record was written:
# "kill the consumer between delivery and commit, assert the subscriber sees the
# same webhook-id twice." The failure-semantics table in that record states it
# as settled -- "Consumer dies after delivering, before commit -> the subscriber
# receives a duplicate ... Accepted; every delivery carries a stable webhook-id
# so subscribers can dedupe" -- and nothing had executed it.
#
# docs/goal-relay.md target behaviour 4 makes the same promise to tenants: the
# contract is at-least-once and a subscriber assuming exactly-once will
# eventually double-charge someone. That is a claim about what relay does when
# it crashes, which is the one condition you cannot check by watching it work.
#
#   make relay-verify-duplicate-on-crash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

COMPOSE=(docker compose -f local/docker-compose.yml)
INGEST="${RELAY_INGEST_URL:-http://localhost:8082}"
SINK="${SINK_URL:-http://localhost:8084}"

# acme, deliberately. It is the one seeded tenant with TWO subscriptions
# (local/bootstrap/relay-db.sh): /hooks/ok, which succeeds, and /hooks/flaky,
# which always fails. The offset commits only once EVERY subscriber for a record
# reaches a terminal state, so flaky burning its retry budget holds the commit
# open long after /hooks/ok has been delivered.
#
# That gap is the whole experiment. Against a single-subscriber tenant the
# commit follows delivery by milliseconds and the window is not reliably
# hittable; here it is seconds wide and the kill lands inside it every time.
TENANT="${TENANT:-acme}"

# The path held open while the kill lands. acme's second subscription always
# fails, so relay is still working on it after /hooks/ok has succeeded -- but
# "still working" used to be a wall-clock guess about retry timing. Latching it
# makes it a state the sink reports.
BLOCKED_PATH="${BLOCKED_PATH:-/hooks/flaky}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }
note() { printf '\033[2m    %s\033[0m\n' "$*"; }

MARKER="crash-$(date +%s)-$$"

cleanup() {
  # Release first: a parked request outlives this script otherwise, and the
  # next run would find the path already latched with something stuck on it.
  curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"release\":\"$BLOCKED_PATH\"}" 2>/dev/null || true
  curl -sf -o /dev/null -X POST "$SINK/control" -d '{"latency_ms":0,"fail_rate":0}' 2>/dev/null || true
  return 0
}
trap cleanup EXIT INT TERM

# webhook-ids delivered to the healthy endpoint for THIS run, in arrival order.
# Deliberately not deduplicated: repetition is the result being measured.
delivered_ids() {
  curl -sf "$SINK/received" \
    | MARKER="$MARKER" python3 -c 'import json,os,sys
marker = os.environ["MARKER"]
d = json.load(sys.stdin)
for x in (d.get("deliveries") or []):
    if x.get("path") != "/hooks/ok" or not (200 <= x.get("status", 0) < 300):
        continue
    data = x.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError:
            continue
    if not isinstance(data, dict) or data.get("marker") != marker:
        continue
    print(x["webhook_id"])'
}

deliver_container() { "${COMPOSE[@]}" ps -q relay-deliver | head -1; }

say "checking relay and the sink are up"
curl -sf "$INGEST/readyz" >/dev/null || fail "relay ingest is not reachable at $INGEST -- run 'make up-apps'"
curl -sf "$SINK/healthz"  >/dev/null || fail "the sink is not reachable at $SINK -- run 'make up-apps'"

ctr=$(deliver_container)
[ -n "$ctr" ] || fail "no relay-deliver container is running -- run 'make up-apps'"

# The latch replaced a latency-based window, so there is no longer a latency to
# validate against RELAY_DELIVERY_TIMEOUT. The timeout still matters for a
# different reason: a parked request is eventually cancelled by relay's own
# per-attempt timeout, so the kill has to land before that. It is reported for
# whoever is reading a failure.
timeout_spec=$(docker inspect "$ctr" --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null |
  sed -n 's/^RELAY_DELIVERY_TIMEOUT=//p')
note "per-attempt delivery timeout is ${timeout_spec:-unset}; the latch must be killed through before it expires"

say "clearing the sink and latching $BLOCKED_PATH"
curl -sf -o /dev/null -X DELETE "$SINK/received" || fail "could not clear $SINK/received"
curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"latch\":\"$BLOCKED_PATH\"}" ||
  fail "could not latch $BLOCKED_PATH through $SINK/control"

say "posting one event for tenant $TENANT"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGEST/v1/events" \
  -H 'content-type: application/json' \
  -d "{\"tenant_id\":\"$TENANT\",\"type\":\"crash.check\",\"data\":{\"marker\":\"$MARKER\"}}")
[ "$code" = "202" ] || fail "ingest returned $code, expected 202"

say "waiting for the healthy subscriber to receive it once"
waited=0
while [ "$(delivered_ids | wc -l | tr -d ' ')" -lt 1 ]; do
  waited=$((waited + 1))
  [ "$waited" -ge 60 ] && fail "the event never reached /hooks/ok, so there is no delivery to duplicate"
  sleep 1
done
first_id=$(delivered_ids | head -1)

# EXACTLY one, not at least one. Record-level replay -- a failed DLQ write, a
# failed commit, a rebalance -- redelivers every subscriber, so a second
# delivery could exist before this script kills anything. Asserting ">= 2"
# afterwards would then pass while attributing someone else's duplicate to this
# kill. Pin the pre-kill count and require it to GROW.
before=$(delivered_ids | grep -c "^${first_id}$")
[ "$before" = "1" ] || fail "expected exactly 1 delivery of $first_id before the kill, found $before.
    Something redelivered this record already, so a duplicate afterwards could
    not be attributed to the kill."

say "waiting for $BLOCKED_PATH to be observably parked"
waited=0
while :; do
  held=$(curl -sf "$SINK/control" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("held",{}).get("'"$BLOCKED_PATH"'",0))' 2>/dev/null || echo 0)
  [ "$held" -ge 1 ] && break
  waited=$((waited + 1))
  [ "$waited" -ge 60 ] && fail "nothing ever parked on $BLOCKED_PATH; relay is not mid-delivery, so the kill would not land between delivery and commit"
  sleep 1
done
note "delivered as $first_id, and relay is parked on $BLOCKED_PATH"
note "the offset cannot commit until every subscriber is finished, so it is held open"

# docker kill, not stop: SIGKILL gives the consumer no chance to commit or shut
# down cleanly, which is the crash this is about. A graceful stop would exercise
# the shutdown path instead, and that path deliberately does NOT commit either.
say "killing the consumer before it can commit"
docker kill "$ctr" >/dev/null 2>&1 || fail "could not kill $ctr"

# Started explicitly rather than waiting on compose's `restart: unless-stopped`.
# Measured on Docker Engine 29.7.2: `docker kill` counts as a MANUAL STOP and
# suppresses that policy, so the container sat Exited (137) with RestartCount=0.
# A SIGKILL a container inflicts on itself is not suppressed. What is suppressed
# is the manual stop, not the signal -- an earlier version of this comment said
# the policy does not survive a SIGKILL at all, which was too broad.
#
# Restarting it by hand is also the more faithful model. What brings a crashed
# consumer back in the environment this demo targets is Kubernetes, not the
# container runtime's own policy.
say "restarting it, as an orchestrator would"
# Confirm the kill actually took before releasing. Releasing first would let the
# parked delivery complete and the offset commit, which is the very thing this
# is trying to prevent.
until [ "$(docker inspect -f '{{.State.Running}}' "$ctr" 2>/dev/null)" = "false" ]; do sleep 0.2; done
note "consumer is stopped; the record is uncommitted with one subscriber delivered"

say "releasing $BLOCKED_PATH"
curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"release\":\"$BLOCKED_PATH\"}" ||
  fail "could not release $BLOCKED_PATH"

docker start "$ctr" >/dev/null 2>&1 || fail "could not restart $ctr"

waited=0
until [ "$(docker inspect -f '{{.State.Running}}' "$ctr" 2>/dev/null)" = "true" ]; do
  waited=$((waited + 1))
  [ "$waited" -ge 60 ] && fail "relay-deliver did not come back within 60s"
  sleep 1
done
note "consumer is back; it resumes from the last committed offset"

# Let the redelivery land. The restarted consumer resumes from the last
# committed offset, which is BEFORE this record.
say "waiting for the same event to be delivered again"
waited=0
while [ "$(delivered_ids | grep -c "^${first_id}$")" -le "$before" ]; do
  waited=$((waited + 1))
  if [ "$waited" -ge 90 ]; then
    note "ids seen on /hooks/ok for this run: $(delivered_ids | tr '\n' ' ')"
    fail "$first_id was delivered $before time(s) and the count never grew.
    The consumer committed before it died, so an event that was delivered but
    not acknowledged would be LOST rather than duplicated -- the opposite of
    what ADR 0006's failure-semantics table claims."
  fi
  sleep 1
done

count=$(delivered_ids | grep -c "^${first_id}$")
distinct=$(delivered_ids | sort -u | wc -l | tr -d ' ')

note "webhook-id $first_id delivered $count times ($before before the kill, $((count - before)) after)"
note "distinct webhook-ids for this run: $distinct"

# The id has to be STABLE across the redelivery. A fresh id per attempt would
# make the duplicate undetectable by a subscriber, which is what target
# behaviour 4 in docs/goal-relay.md promises against.
if [ "$distinct" -ne 1 ]; then
  fail "the redelivery carried a different webhook-id ($distinct distinct ids).
    At-least-once is only usable because the id is stable -- a subscriber
    deduping on webhook-id would not have caught this."
fi

pass "a crash between delivery and commit redelivered the SAME webhook-id ($before -> $count)"

cat <<'EOF'

  This is at-least-once working as designed, not a defect. The subscriber's
  side of the contract is deduping on webhook-id; goal-relay.md target
  behaviour 4 says so, and ADR 0006 accepts the duplicate over the alternative,
  which is losing the event.
EOF

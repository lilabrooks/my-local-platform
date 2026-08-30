#!/usr/bin/env bash
# Prove that events for one tenant are delivered in the order they were
# accepted.
#
# This is target behaviour 3 in docs/goal-relay.md, and one of the three
# properties the goal document leads with. ADR 0006 listed it under "Still
# planned" from the day the record was written -- "produce a known sequence for
# one tenant, assert delivery order at the sink" -- and nothing executed it.
# See issue #54.
#
# What this covers, precisely: the steady state, one consumer, no membership
# change. That is the weaker half of the question and it is the half that has
# to pass first. Ordering ACROSS a rebalance is the other half, and it needs a
# second consumer; it is not asserted here.
#
#   make relay-verify-ordering
set -euo pipefail

INGEST="${RELAY_INGEST_URL:-http://localhost:8082}"
SINK="${SINK_URL:-http://localhost:8084}"
EVENTS="${EVENTS:-40}"

# globex, not acme. acme carries a second subscription pointed at /hooks/flaky
# (local/bootstrap/relay-db.sh), and the consumer commits only once every
# subscriber for a record reaches a terminal state -- so every record would
# wait out the whole retry budget before the next one moved. globex has exactly
# one healthy subscriber, which is what makes this measure ordering rather than
# measuring the retry schedule.
TENANT="${TENANT:-globex}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }

# A marker unique to this run. DELETE /received clears history, but the smoke
# check and any concurrent demo also deliver to /hooks/ok, and a stray delivery
# landing mid-run would be indistinguishable from a reordered one. Filtering to
# this run's marker makes the assertion about this run's events only.
MARKER="ordering-$(date +%s)-$$"

# Sequence numbers, in the order the sink received them, for this run only.
delivered_seqs() {
  curl -sf "$SINK/received" \
    | MARKER="$MARKER" python3 -c 'import json,os,sys
marker = os.environ["MARKER"]
d = json.load(sys.stdin)
for x in (d.get("deliveries") or []):
    if x.get("path") != "/hooks/ok":
        continue
    if not (200 <= x.get("status", 0) < 300):
        continue
    # delivery.Data is a json.RawMessage, so it arrives as a nested object
    # rather than a string. Tolerate both instead of assuming.
    data = x.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError:
            continue
    if not isinstance(data, dict) or data.get("marker") != marker:
        continue
    print(data["seq"])'
}

say "checking relay and the sink are up"
curl -sf "$INGEST/readyz" >/dev/null || fail "relay ingest is not reachable at $INGEST -- run 'make up-apps'"
curl -sf "$SINK/healthz"  >/dev/null || fail "the sink is not reachable at $SINK -- run 'make up-apps'"

# A sink still slowed from an earlier demo run would make this time out for a
# reason that has nothing to do with ordering.
say "resetting sink latency and failure rate"
curl -sf -o /dev/null -X POST "$SINK/control" -d '{"latency_ms":0,"fail_rate":0}' ||
  fail "could not reset the sink through $SINK/control"

say "clearing the sink's delivery history"
curl -sf -o /dev/null -X DELETE "$SINK/received" || fail "could not clear $SINK/received"

# Sequentially, waiting for each 202 before sending the next. Concurrent POSTs
# would leave the accepted order undefined, and an assertion about delivery
# order is meaningless without a known accepted order to compare it against.
say "posting $EVENTS events for tenant $TENANT, one at a time"
for i in $(seq 1 "$EVENTS"); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGEST/v1/events" \
    -H 'content-type: application/json' \
    -d "{\"tenant_id\":\"$TENANT\",\"type\":\"ordering.check\",\"data\":{\"seq\":$i,\"marker\":\"$MARKER\"}}")
  [ "$code" = "202" ] || fail "ingest returned $code for event $i, expected 202"
done

say "waiting for all $EVENTS to reach the sink"
waited=0
while :; do
  n=$(delivered_seqs | wc -l | tr -d ' ')
  [ "$n" -ge "$EVENTS" ] && break
  waited=$((waited + 1))
  [ "$waited" -ge 60 ] && fail "only $n of $EVENTS were delivered within 60s"
  sleep 1
done

say "asserting delivery order matches accepted order"
got=$(delivered_seqs | tr '\n' ' ' | sed 's/ $//')
want=$(seq 1 "$EVENTS" | tr '\n' ' ' | sed 's/ $//')

if [ "$got" != "$want" ]; then
  # Print the first divergence rather than two long lines the reader has to
  # diff by eye.
  first_bad=$(python3 -c '
import sys
got = sys.argv[1].split()
want = sys.argv[2].split()
for i, (g, w) in enumerate(zip(got, want)):
    if g != w:
        print(f"position {i}: delivered seq {g}, expected {w}")
        break
else:
    print(f"lengths differ: {len(got)} delivered, {len(want)} expected")
' "$got" "$want")
  printf '  %s\n' "$first_bad" >&2
  printf '  delivered: %s\n' "$got" >&2
  fail "tenant $TENANT received its events out of order"
fi

pass "$EVENTS events for $TENANT delivered in the order they were accepted"

cat <<'EOF'

  Steady state only: one consumer, no membership change. Ordering across a
  rebalance is the other half of issue #54: run
  scripts/verify-ordering-rebalance.sh, which starts a second consumer and
  waits for the broker to report the group has actually changed generation.
EOF

#!/usr/bin/env bash
# Does per-tenant ordering survive a consumer-group rebalance?
#
# The second half of issue #54. scripts/verify-ordering.sh covers the steady
# state -- one consumer, no membership change -- and passes. This one changes
# group membership while deliveries are in flight, which is what KEDA does
# routinely during the M2 demo.
#
# The hypothesis being tested, from #54:
#
#   relay passes ONE context, from signal.NotifyContext, all the way down to
#   Deliver (cmd/relay/main.go:174). Nothing cancels it when the consumer
#   group changes generation, because kafka-go handles joins and generation
#   changes on background goroutines that never touch a caller's context. So
#   an old partition owner can still be mid-POST when the partition has
#   already moved. The new owner resumes from the last committed offset and
#   moves on; the old owner's in-flight delivery lands afterwards.
#
# Expected shape of a violation at the sink: ... 11, 12, 11 ...
#
# What is NOT a violation: a repeated value in place, such as 11, 11, 12. The
# contract is at-least-once (docs/goal-relay.md target behaviour 4), duplicates
# carry a stable webhook-id for deduping, and ADR 0006:225 accepts them
# explicitly. Only a DECREASE breaks ordering, so consecutive duplicates are
# collapsed before the assertion.
#
#   ./scripts/verify-ordering-rebalance.sh
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

COMPOSE=(docker compose -f local/docker-compose.yml)
INGEST="${RELAY_INGEST_URL:-http://localhost:8082}"
SINK="${SINK_URL:-http://localhost:8084}"
EVENTS="${EVENTS:-60}"
TENANT="${TENANT:-globex}"

# Slow enough that deliveries are still in flight when membership changes.
# At 0ms the whole backlog drains before the second consumer has joined, and
# the rebalance lands on an idle group -- which proves nothing. Kept under
# RELAY_DELIVERY_TIMEOUT so attempts do not fail for an unrelated reason.
LATENCY_MS="${LATENCY_MS:-1500}"

# Seconds to let the first consumer work before adding the second.
SCALE_AFTER="${SCALE_AFTER:-7}"

# Drop sink latency to zero the moment the group has two members.
#
# This is what opens the window the mechanism needs, and a uniform latency
# closes it. With both consumers equally slow, the old owner's in-flight
# attempt for record N finishes in at most L, while the new owner needs 2L to
# deliver N and then N+1 -- so the late arrival almost always lands before the
# record that would make it a violation. Releasing the sink at handover makes
# the new owner fast while the old owner is still asleep in a POST it began
# when the sink was slow (the handler reads its latency once, at request
# start, so an in-flight request keeps the old duration).
#
# That is not a contrived sequence. It is the M2 demo: slow the sink, let KEDA
# add consumers, release the sink.
#
# RELEASE=0 keeps the latency uniform, which is the weaker test.
RELEASE="${RELEASE:-1}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }
note() { printf '\033[2m    %s\033[0m\n' "$*"; }

MARKER="rebal-$(date +%s)-$$"

SECOND=""
cleanup() {
  say "removing the second consumer and restoring a fast sink"
  [ -n "$SECOND" ] && docker rm -f "$SECOND" >/dev/null 2>&1
  curl -sf -o /dev/null -X POST "$SINK/control" -d '{"latency_ms":0,"fail_rate":0}' 2>/dev/null || true
  return 0
}
trap cleanup EXIT INT TERM

# Group members, as the broker sees them. This is the difference between "a
# container started" and "the consumer group actually changed generation".
#
# The tool prints a BLANK line, then a header, then one row per member. `NR > 1`
# skips the blank and counts the header, which reports one member as two -- and
# would have let the two-member wait below succeed against a single consumer.
members() {
  docker exec mlp-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --group relay-deliver --describe --members 2>/dev/null |
    awk 'NF > 0 && $1 != "GROUP"'
}

delivered_seqs() {
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
    print(data["seq"])'
}

say "checking relay and the sink are up"
curl -sf "$INGEST/readyz" >/dev/null || fail "relay ingest is not reachable at $INGEST -- run 'make up-apps'"
curl -sf "$SINK/healthz"  >/dev/null || fail "the sink is not reachable at $SINK -- run 'make up-apps'"

say "checking the group has exactly one member to start from"
before=$(members | wc -l | tr -d ' ')
[ "$before" = "1" ] || fail "expected 1 group member to start from, found $before -- remove any extra relay-deliver containers first"
note "$(members | awk '{print $NF" partitions held by "$1}')"

# A latency at or above RELAY_DELIVERY_TIMEOUT makes every attempt time out and
# dead-letter, so nothing reaches /hooks/ok and the run fails looking like a
# broken consumer. relay-demo.sh guards the same way for the same reason; this
# reads the value off the running container rather than restating it, because
# a number copied here would drift from the one in compose.
deliver_ctr=$("${COMPOSE[@]}" ps -q relay-deliver | head -1)
timeout_spec=$(docker inspect "$deliver_ctr" \
  --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null |
  sed -n 's/^RELAY_DELIVERY_TIMEOUT=//p')
case "$timeout_spec" in
  *ms) timeout_ms="${timeout_spec%ms}" ;;
  *s)  timeout_ms=$(( ${timeout_spec%s} * 1000 )) ;;
  *)   timeout_ms=0 ;;
esac
if [ "$timeout_ms" -gt 0 ] && [ "$LATENCY_MS" -ge "$timeout_ms" ]; then
  fail "LATENCY_MS=${LATENCY_MS} is not below RELAY_DELIVERY_TIMEOUT=${timeout_spec}.
    Every attempt would time out and dead-letter, so nothing would reach
    /hooks/ok and this would look like a broken consumer. Pick a latency
    under ${timeout_ms}ms."
fi
note "delivery timeout is ${timeout_spec}; ${LATENCY_MS}ms is under it"

say "sink latency ${LATENCY_MS}ms, so deliveries are in flight during the rebalance"
curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"latency_ms\":$LATENCY_MS,\"fail_rate\":0}" ||
  fail "could not slow the sink through $SINK/control"

say "clearing the sink's delivery history"
curl -sf -o /dev/null -X DELETE "$SINK/received" || fail "could not clear $SINK/received"

# Posted up front, so a backlog exists for the consumer to work through and the
# rebalance has something to interrupt.
say "posting $EVENTS events for tenant $TENANT"
for i in $(seq 1 "$EVENTS"); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGEST/v1/events" \
    -H 'content-type: application/json' \
    -d "{\"tenant_id\":\"$TENANT\",\"type\":\"ordering.rebalance\",\"data\":{\"seq\":$i,\"marker\":\"$MARKER\"}}")
  [ "$code" = "202" ] || fail "ingest returned $code for event $i, expected 202"
done

say "letting the consumer get into the backlog (${SCALE_AFTER}s)"
sleep "$SCALE_AFTER"
mid=$(delivered_seqs | wc -l | tr -d ' ')
note "$mid of $EVENTS delivered before the membership change"
[ "$mid" -gt 0 ]        || fail "nothing delivered in ${SCALE_AFTER}s -- the consumer is not working, so this would not test a rebalance"
[ "$mid" -lt "$EVENTS" ] || fail "the whole backlog drained in ${SCALE_AFTER}s -- raise LATENCY_MS or EVENTS, or this proves nothing"

# `compose run`, not `up --scale`: relay-deliver publishes host port 8083, and
# a second instance of the service cannot bind it -- scaling fails with "port
# is already allocated". `run` does not publish the service's ports unless
# asked with --service-ports, and a second consumer needs no inbound traffic
# anyway, which is what the port mapping's own comment says.
say "adding a second consumer -- this is the rebalance"
SECOND=$("${COMPOSE[@]}" run -d --no-deps relay-deliver)
[ -n "$SECOND" ] || fail "could not start a second consumer"

say "waiting for the broker to report two members"
waited=0
while [ "$(members | wc -l | tr -d ' ')" -lt 2 ]; do
  waited=$((waited + 1))
  [ "$waited" -ge 45 ] && fail "the group never reached 2 members -- no rebalance happened, so this proves nothing"
  sleep 1
done
note "group rebalanced to $(members | wc -l | tr -d ' ') members:"
members | while IFS= read -r m; do note "  $(echo "$m" | awk '{print $NF" partitions"}')"; done

if [ "$RELEASE" = "1" ]; then
  say "releasing the sink -- the new owner is now fast, the old one is still mid-POST"
  curl -sf -o /dev/null -X POST "$SINK/control" -d '{"latency_ms":0,"fail_rate":0}' ||
    fail "could not release the sink through $SINK/control"
fi

say "waiting for all $EVENTS distinct events to reach the sink"
waited=0
while :; do
  n=$(delivered_seqs | sort -un | wc -l | tr -d ' ')
  [ "$n" -ge "$EVENTS" ] && break
  waited=$((waited + 1))
  [ "$waited" -ge 120 ] && fail "only $n of $EVENTS distinct events arrived within 120s"
  sleep 1
done

say "asserting the delivered sequence never goes backwards"
seqs=$(delivered_seqs | tr '\n' ' ' | sed 's/ $//')

result=$(python3 -c '
import sys
seqs = [int(x) for x in sys.argv[1].split()]

# Collapse consecutive duplicates: at-least-once makes 11, 11, 12 correct.
collapsed = [s for i, s in enumerate(seqs) if i == 0 or s != seqs[i - 1]]

violations = []
for i in range(1, len(collapsed)):
    if collapsed[i] < collapsed[i - 1]:
        violations.append((i, collapsed[i - 1], collapsed[i]))

print(f"total={len(seqs)} distinct={len(set(seqs))} duplicates={len(seqs) - len(set(seqs))}")
if violations:
    print("VIOLATIONS " + str(len(violations)))
    for pos, prev, cur in violations[:10]:
        print(f"  position {pos}: seq {cur} delivered after seq {prev}")
else:
    print("CLEAN")
' "$seqs")

printf '%s\n' "$result" | while IFS= read -r line; do note "$line"; done

if printf '%s' "$result" | grep -q VIOLATIONS; then
  cat <<EOF

  Ordering did NOT survive the rebalance. This is the failure predicted in
  issue #54: an old partition owner completed a delivery after the new owner
  had already moved past it.

  Record this in ADR 0006's consequences and the backlog -- NOT by editing
  goal-relay.md's target behaviour, which is a prediction and stays as
  written (AGENTS.md: update the status, not the substance).
EOF
  fail "tenant $TENANT received events out of order across a rebalance"
fi

pass "ordering survived the rebalance: no delivered sequence went backwards"
note "Duplicates are expected and are not a violation; see ADR 0006:225."

cat <<EOF

  What a pass here does NOT establish. The mechanism in #54 is real -- nothing
  cancels the delivery context when the group changes generation -- so this
  says the window is hard to hit with this configuration, not that it is
  closed.

  One reason it is hard, which is a hypothesis this script does not measure:
  RELAY_DELIVERY_TIMEOUT (${timeout_spec}) caps a single attempt, while joining a
  group takes seconds. The old owner's in-flight attempt therefore tends to
  finish BEFORE the new owner resumes, and a late arrival that lands before
  the next record is not a reordering. The case that would still bite is an
  old owner asleep in retry backoff across the whole rebalance, which
  RELAY_RETRY_DELAYS bounds -- and which config.ValidateLiveness already
  rejects when the total grows past the rebalance timeout.

  If that hypothesis holds, ValidateLiveness bounds more than the liveness it
  is named for. Confirming it needs instrumentation at the handover, not more
  runs of this script.
EOF

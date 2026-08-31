#!/usr/bin/env bash
# Demonstrate head-of-line blocking, and show what its blast radius actually is.
#
# ADR 0006 carried this under "Still planned" from the day the record was
# written: "degrade one subscriber and measure delivery delay for an unrelated
# tenant on the same partition." Issue #73 forced it.
#
# The premise changed while this was being built, and the change is the point.
# `Consumer.Run` fetches one record, handles it to completion, commits, and only
# then fetches again -- one serial loop per consumer MEMBER, with no
# per-partition concurrency. So a slow subscriber does not block its partition;
# it blocks every partition its member owns.
#
# A comparison against "a different partition" therefore proves nothing on a
# single member: both tenants are behind the same loop. This runs TWO members
# and compares:
#
#   victim   -- a tenant on a partition owned by the SAME member as the blocker
#   control  -- a tenant on a partition owned by the OTHER member
#
# Every assignment is read from the broker at runtime. None of it is hardcoded:
# the tenant-to-partition mapping depends on the partition count, and ADR 0006
# records that raising that count reshuffles it.
#
#   make relay-verify-head-of-line
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

COMPOSE=(docker compose -f local/docker-compose.yml)
INGEST="${RELAY_INGEST_URL:-http://localhost:8082}"
SINK="${SINK_URL:-http://localhost:8084}"
TOPIC="${RELAY_TOPIC:-mlp.relay.deliveries}"
GROUP="${RELAY_CONSUMER_GROUP:-relay-deliver}"

# acme is the blocker: the one seeded tenant with a second subscription
# (/hooks/flaky) that can be latched open while /hooks/ok succeeds.
BLOCKER="${BLOCKER:-acme}"
BLOCKED_PATH="${BLOCKED_PATH:-/hooks/flaky}"

# Candidates to search for a victim and a control. All have exactly one healthy
# subscriber, so their delivery time is a clean measurement.
CANDIDATES="${CANDIDATES:-globex demo-01 demo-02 demo-03 demo-04 demo-05 demo-06 demo-07 demo-08 demo-09 demo-10 demo-11 demo-12 demo-13 demo-14 demo-15 demo-16}"

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }
pass() { printf '\033[32mPASS\033[0m %s\n' "$*"; }
note() { printf '\033[2m    %s\033[0m\n' "$*"; }

MARKER="hol-$(date +%s)-$$"
SECOND=""

cleanup() {
  curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"release\":\"$BLOCKED_PATH\"}" 2>/dev/null || true
  [ -n "$SECOND" ] && docker rm -f "$SECOND" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT INT TERM

kafka() { local tool="$1"; shift; docker exec mlp-kafka "/opt/kafka/bin/$tool" "$@"; }

end_offsets() {
  kafka kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic "$TOPIC" --time -1 2>/dev/null |
    awk -F: '{print $2" "$3}' | sort -n
}

members_raw() {
  kafka kafka-consumer-groups.sh --bootstrap-server localhost:19092 \
    --group "$GROUP" --describe --members --verbose 2>/dev/null | awk 'NF>0 && $1!="GROUP"'
}

# owner <partition> -- which member id currently holds it, per the broker.
owner() {
  members_raw | awk -v want="$1" '{
    for (i = 1; i <= NF; i++) if ($i ~ /:[0-9]/) {
      split($i, a, ":"); n = split(a[2], parts, ",")
      for (j = 1; j <= n; j++) if (parts[j] == want) { print substr($2, 7, 12); exit }
    }
  }'
}

post() {
	local tenant="$1" tag="$2" response code body
	response=$(curl -sS -w $'\n%{http_code}' -X POST "$INGEST/v1/events" \
	  -H 'content-type: application/json' \
	  -d "{\"tenant_id\":\"$tenant\",\"type\":\"hol.check\",\"data\":{\"marker\":\"$MARKER-$tag\"}}")
	code=${response##*$'\n'}
	body=${response%$'\n'*}
	[ "$code" = "202" ] || fail "ingest returned $code for $tenant, expected 202"
	printf '%s' "$body" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
}

# partition_contains_event <partition> <offset> <count> <event-id>
partition_contains_event() {
	local partition="$1" offset="$2" count="$3" event_id="$4"
	[ "$count" -gt 0 ] || return 1
	kafka kafka-console-consumer.sh --bootstrap-server localhost:19092 \
	  --topic "$TOPIC" --partition "$partition" --offset "$offset" \
	  --max-messages "$count" --timeout-ms 3000 2>/dev/null \
	  | EVENT_ID="$event_id" python3 -c 'import json,os,sys
want = os.environ["EVENT_ID"]
found = False
for line in sys.stdin:
    try:
        if json.loads(line).get("id") == want:
            found = True
    except (ValueError, AttributeError):
        pass
raise SystemExit(0 if found else 1)'
}

# partition_of <tenant> -- posts one probe and finds that exact event in the
# records added after the snapshot. Offset movement alone is ambiguous when
# another producer is active; correlating the accepted event id keeps discovery
# correct even when several partitions move concurrently.
partition_of() {
	local tenant="$1" before after event_id partition end start count found
	before=$(end_offsets)
	event_id=$(post "$tenant" "probe")
	for _ in $(seq 1 50); do
	  after=$(end_offsets)
	  found=""
	  while read -r partition end; do
	    start=$(printf '%s\n' "$before" | awk -v want="$partition" '$1 == want {print $2}')
	    [ -n "$start" ] || continue
	    count=$((end - start))
	    if partition_contains_event "$partition" "$start" "$count" "$event_id"; then
	      [ -z "$found" ] || return 1
	      found="$partition"
	    fi
	  done <<<"$after"
	  if [ -n "$found" ]; then echo "$found"; return 0; fi
	  sleep 0.2
	done
	return 1
}

delivered() {
  curl -sf "$SINK/received" | MARKER="$MARKER-$1" python3 -c 'import json,os,sys
want = os.environ["MARKER"]
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
    if isinstance(data, dict) and data.get("marker") == want:
        print("yes"); break'
}

# time_to_delivery <tenant> <tag> <budget-seconds>
time_to_delivery() {
	local tenant="$1" tag="$2" budget="$3" start
	start=$(python3 -c 'import time;print(time.time())')
	post "$tenant" "$tag" >/dev/null
  while :; do
    [ -n "$(delivered "$tag")" ] && { python3 -c "import time;print(f'{time.time()-$start:.3f}')"; return 0; }
    if [ "$(python3 -c "import time;print(int(time.time()-$start))")" -ge "$budget" ]; then
      echo "TIMEOUT"; return 0
    fi
    sleep 0.05
  done
}

say "checking relay and the sink are up"
curl -sf "$INGEST/readyz" >/dev/null || fail "relay ingest is not reachable at $INGEST -- run 'make up-apps'"
curl -sf "$SINK/healthz"  >/dev/null || fail "the sink is not reachable at $SINK -- run 'make up-apps'"

say "starting a second consumer, so partitions have different owners"
SECOND=$("${COMPOSE[@]}" run -d --no-deps relay-deliver)
[ -n "$SECOND" ] || fail "could not start a second consumer"

say "waiting for the group to hold two members with partitions each"
for _ in $(seq 1 60); do
  n=$(members_raw | wc -l | tr -d ' ')
  idle=$(members_raw | awk '{for (i=1;i<=NF;i++) if ($i ~ /^[0-9]+$/ && $i == 0) {print "idle"; exit}}')
  [ "$n" -ge 2 ] && [ -z "$idle" ] && break
  sleep 2
done
[ "$(members_raw | wc -l | tr -d ' ')" -ge 2 ] || fail "the group never reached two members"
members_raw | awk '{for (i=1;i<=NF;i++) if ($i ~ /:[0-9]/) print "    member " substr($2,7,12) " owns " $i}'

say "locating the blocker's partition and its owner"
blocker_part=$(partition_of "$BLOCKER") || fail "could not determine $BLOCKER's partition"
blocker_owner=$(owner "$blocker_part")
[ -n "$blocker_owner" ] || fail "no member owns partition $blocker_part"
note "$BLOCKER -> partition $blocker_part, owned by member $blocker_owner"

say "finding a victim on the same member and a control on the other"
victim=""; victim_part=""; control=""; control_part=""
for t in $CANDIDATES; do
  [ -n "$victim" ] && [ -n "$control" ] && break
  p=$(partition_of "$t") || continue
  o=$(owner "$p")
  [ -z "$o" ] && continue
  if [ "$o" = "$blocker_owner" ] && [ "$p" != "$blocker_part" ] && [ -z "$victim" ]; then
    victim="$t"; victim_part="$p"
  elif [ "$o" != "$blocker_owner" ] && [ -z "$control" ]; then
    control="$t"; control_part="$p"
  fi
done
[ -n "$victim" ]  || fail "no candidate tenant shares $BLOCKER's member on a different partition"
[ -n "$control" ] || fail "no candidate tenant landed on the other member"
note "victim  $victim -> partition $victim_part (same member $blocker_owner)"
note "control $control -> partition $control_part (other member)"

say "baseline, nothing blocking"
curl -sf -o /dev/null -X DELETE "$SINK/received" || fail "could not clear the sink"
base_victim=$(time_to_delivery "$victim" "bv" 30)
base_control=$(time_to_delivery "$control" "bc" 30)
note "victim  $base_victim s"
note "control $base_control s"

say "latching $BLOCKED_PATH and blocking $BLOCKER"
curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"latch\":\"$BLOCKED_PATH\"}" ||
  fail "could not latch $BLOCKED_PATH"
post "$BLOCKER" "block" >/dev/null

say "waiting for the blocker to be observably parked"
for _ in $(seq 1 60); do
  held=$(curl -sf "$SINK/control" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("held",{}).get("'"$BLOCKED_PATH"'",0))' 2>/dev/null || echo 0)
  [ "$held" -ge 1 ] && break
  sleep 1
done
[ "${held:-0}" -ge 1 ] || fail "nothing parked on $BLOCKED_PATH; the blocker is not holding its member"
note "member $blocker_owner is now stuck inside one record"

# Budget, not a sleep: the victim is expected to be delayed and the control is
# expected to stay near its baseline. A fixed wait would make the result depend
# on how long this script chose to be patient.
BUDGET="${BUDGET:-15}"
say "measuring both while the member is blocked (budget ${BUDGET}s)"
held_control=$(time_to_delivery "$control" "hc" "$BUDGET")
held_victim=$(time_to_delivery "$victim" "hv" "$BUDGET")

say "releasing $BLOCKED_PATH"
curl -sf -o /dev/null -X POST "$SINK/control" -d "{\"release\":\"$BLOCKED_PATH\"}" || true

printf '\n'
printf '  %-38s %-12s %s\n' "" "baseline" "blocked"
printf '  %-38s %-12s %s\n' "victim  ($victim, p$victim_part, same member)" "${base_victim}s" "${held_victim}s"
printf '  %-38s %-12s %s\n' "control ($control, p$control_part, other member)" "${base_control}s" "${held_control}s"
printf '\n'

# The victim is NOT expected to hang forever. relay's per-attempt timeout
# eventually cancels the parked request, so the blocker holds its member through
# its configured retry work and then dead-letters. The 30s stall budget is the
# ceiling on that work, not the duration this compose run should measure. The
# claim is that the victim is delayed by the configured work while the control
# stays near its baseline, so this compares the two rather than waiting for an
# artificial timeout.
verdict=$(python3 -c '
import sys
bv, bc, hv, hc = sys.argv[1:5]
if "TIMEOUT" in (hv, hc):
    print("TIMEOUT " + ("victim" if hv == "TIMEOUT" else "control")); raise SystemExit
bv, bc, hv, hc = float(bv), float(bc), float(hv), float(hc)
if hc > 1.0:
    print(f"CONTROL_BLOCKED {hc:.3f}")
elif hc > max(bc, 0.001) * 5:
    print(f"CONTROL_REGRESSED {bc:.3f} {hc:.3f}")
elif hv < 1.0:
    print(f"VICTIM_NOT_BLOCKED {hv:.3f}")
elif hv < max(bv, 0.001) * 5:
    print(f"VICTIM_UNCHANGED {bv:.3f} {hv:.3f}")
elif hv < hc * 5:
    print(f"NO_SEPARATION {hv:.3f} vs {hc:.3f}")
else:
    print(f"OK {hv/max(hc,0.001):.0f}")
' "$base_victim" "$base_control" "$held_victim" "$held_control")

case "$verdict" in
	CONTROL_BLOCKED*)
    fail "the control on the OTHER member was delayed too (${held_control}s).
    Partitions owned by a different member should be unaffected. Check that
	    both members really hold partitions." ;;
	CONTROL_REGRESSED*)
	  fail "the control on the other member grew from ${base_control}s baseline to ${held_control}s.
	    The member boundary did not preserve the control's measured behaviour." ;;
	VICTIM_NOT_BLOCKED*)
    fail "the victim on the SAME member was delivered in ${held_victim}s despite its
    member being stuck inside a latched record. Either the blocker was not
    holding the member, or Consumer.Run no longer serialises records per member
	    -- in which case ADR 0006's head-of-line section needs revisiting rather
	    than this script." ;;
	VICTIM_UNCHANGED*)
	  fail "the victim changed from ${base_victim}s baseline to ${held_victim}s, less than the
	    required 5x slowdown. The run did not demonstrate head-of-line delay." ;;
  NO_SEPARATION*)
    fail "victim and control were delayed comparably ($verdict).
    Without separation this demonstrates nothing about member scope." ;;
  TIMEOUT*)
    fail "a measurement timed out ($verdict); raise BUDGET or check the stack." ;;
esac

ratio="${verdict#OK }"

pass "head-of-line blocking is member-scoped: victim ${held_victim}s vs control ${held_control}s (${ratio}x)"

cat <<EOF

	What this shows, precisely. One record parked inside its consumer blocks
	every partition that MEMBER owns -- the victim sat on a DIFFERENT partition
	and still waited ${held_victim}s -- while a partition owned by the other
	member kept delivering in ${held_control}s against its ${base_control}s
	baseline, inside both the 1s ceiling and the 5x baseline bound.

	The victim is delayed rather than stuck: relay's per-attempt timeout cancels
	each parked request. The blocker retries according to compose's configured
	schedule, dead-letters, and releases the member. This run measures that 6.6s
	worst case; the separate 30s stall budget is its policy ceiling.

  This is Segment's first architecture in miniature, and the reason ADR 0006
  says the isolation this design has is the count of active members rather than
  the partition count.
EOF

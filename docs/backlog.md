# Backlog

**This is not a mirror of the issue tracker.** GitHub tracks *work*; this file
records *why something was deliberately not done yet*, and what would change
that. Most open issues never belong here — they are scheduled, and a milestone
already says so.

An entry earns its place when the decision to defer is itself the thing worth
keeping: a defect somebody chose to live with, and the condition under which
that choice stops holding. Each one says what is wrong, why it is not fixed,
and what "done" looks like — so the reasoning survives without the conversation
that produced it, and without network access.

**A trigger has to be something that can actually be observed, and where it
cannot be, the entry says so.** A condition nobody can detect is not a deferral;
it is a way of never returning to something, wearing the costume of a plan.
Issue #21's entry below has a section on this, because two of its triggers
failed that test before the third admitted it.

Anything scheduled into a milestone belongs on the tracker alone. Resolved
entries are removed rather than archived here; the commit that closes an issue
is the record, and an ADR carries the measurements where there are any.

## Open

### relay delivery targets are not checked against internal addresses

**Issue:** [#16](https://github.com/lilabrooks/my-local-platform/issues/16) ·
**Found:** 2026-08-25 · **Address:** before anything lets tenants register their
own endpoints

`subscriptions.ValidateURL` checks that a delivery URL parses, uses http or
https, and has a host. Nothing else. A subscription pointing at
`http://169.254.169.254/latest/meta-data/`, `http://localhost:9200` or an
RFC1918 address is delivered to, and relay makes those requests from inside the
cluster network, on a schedule, with retries, signed.

Not exploitable today: subscriptions are operator-seeded configuration in
`local/bootstrap/relay-db.sh` and nothing writes the table at runtime, so every
target was chosen by whoever ran the seed. That is the only thing holding it,
which is why the trigger above is a change rather than a date.

The dynamic half is already closed — `delivery.NewDeliverer` sets `CheckRedirect`
to refuse, so a subscriber cannot bounce a signed payload elsewhere at request
time. This is the static half: the configured URL itself.

Done means the resolved IP is checked at dial time, not the hostname string. A
hostname check is bypassed by DNS resolving to a private address, and
re-resolving after checking is a TOCTOU gap; `net.Dialer.Control` sees the
address actually being dialled. A refused target should dead-letter immediately
rather than retry, since the address will not become public on attempt three.
The local stack legitimately delivers to `http://sink:8081`, so the check needs
an allowlist rather than an off switch. Full acceptance criteria are in the
issue.

### relay deliver readiness does not reflect partition assignment

**Issue:** [#21](https://github.com/lilabrooks/my-local-platform/issues/21) ·
**Found:** 2026-08-25 · **Deferred:** 2026-08-26, behind
[#32](https://github.com/lilabrooks/my-local-platform/issues/32) ·
**Address:** on a decision to, because **nothing will detect this on its own
until the fix itself is in** — see the trigger note below

`Consumer.Run` marks itself ready the instant the loop starts — before joining
the consumer group and before any partition assignment. A consumer holding zero
partitions reports `Ready`, and a Kubernetes readiness probe passes it.

This is the bug CI caught in #10, where relay-deliver joined a group before its
topic existed, was assigned nothing, and sat healthy and idle while every event
went undelivered. It would have reported Ready throughout.

Deferred deliberately, with the demo sequenced ahead of it. The reasoning: KEDA
scaling to twelve pods that all report Ready while lag does not drain would
make the demo show autoscaling that visibly does nothing — but that is a
failure the demo would *surface*, loudly, rather than hide. Fixing it first
would be fixing a predicted problem; running the demo tells us whether it is a
real one.

**The demo has since run, and did not surface it.**
[ADR 0008's Verification](adr/0008-in-cluster-observability-for-the-demo.md#verification)
records the full cycle on 2026-08-27: 600 events across 16 tenants, scaling 1 →
12 replicas, lag 596 → 0, every event delivered and no dead letters. Twelve pods
reporting Ready while lag did not drain is precisely what did not happen.

That answers the question the deferral posed and does **not** resolve the
defect. `Consumer.Run` still marks itself ready before joining a generation, and
`relay_topic_partitions_unassigned` still does not exist. One demo draining
shows the predicted symptom did not appear in that run; it cannot show that an
idle-Ready pod is impossible, because — as the trigger note below explains at
length — nothing in the stack can distinguish one from outside.

**Smaller than it was, and reshaped on 2026-08-27.** Two things changed it.

`WatchPartitionChanges: true` with a 2s interval is already set in
`cmd/relay/main.go`, added for exactly the #10 scenario, so a consumer that
joins before its topic exists now self-corrects within two seconds. What is left
is an imprecise signal, not a stuck consumer.

And the original shape was not implementable: `kafka-go`'s `Reader` does not
expose its assignment. Pursuing it reached "which member am I?", which is a
symptom of putting a group-level property in a per-pod place — the thing
[ADR 0008](adr/0008-in-cluster-observability-for-the-demo.md) already settled
for lag, by having ingest publish it rather than the consumers.

Done now means two separate things, split by who can answer:

- **Per-pod, locally:** `/readyz` reflects having joined a generation and begun
  fetching, rather than "the goroutine started". `/healthz` stays independent.
  A consumer holding zero partitions is deliberately **still ready** — it takes
  no inbound traffic, so unready buys nothing, and flapping through a rebalance
  can stall a rolling update.
- **Group-wide, from ingest's existing broker client:**
  `relay_topic_partitions_unassigned`. That is the #10 condition, and no per-pod
  probe can express it, because a partition owned by nobody is invisible to
  every pod individually.

  **This metric does not exist yet, and is not the one that shipped.**
  `relay_lag_partitions_missing` was added to the same `LagPoller` on
  2026-08-30, and the names are close enough to mislead. It counts partitions
  whose lag could not be *read* on the last poll — a broker-side failure, which
  is why an incomplete poll no longer publishes a total. `..._unassigned` would
  count partitions the group has *no member for*, which is a healthy read of a
  broken assignment. A partition can be unassigned and perfectly readable, so
  the existing gauge does not cover this and reusing it would hide the very
  condition #10 produced.

### The trigger, and why this one is honest about having none

This entry has now had three triggers, and the first two were worse than no
trigger at all.

*"If the demo shows pods that are Ready and idle"* was **unfalsifiable by
watching**: an idle-Ready pod looks exactly like a busy one from outside, which
is the whole defect. Nobody could ever have observed it.

*"If a partition is left with no owner, which is now measurable"* replaced it and
was **circular**: the thing that would make it measurable is
`relay_topic_partitions_unassigned`, which is part of this issue's own fix. The
trigger depended on the work it was meant to schedule.

So this one says the true thing instead. **Nothing in the stack will surface
this condition until the metric exists, and the metric is the fix.** It will be
picked up because someone decides to, not because something goes red. Anyone
relying on being told is relying on nothing.

That is an acceptable state for a defect whose practical impact is currently
mitigated -- `WatchPartitionChanges` self-corrects the #10 scenario within two
seconds -- and it would not be acceptable for one that was not. The distinction
is the reason to write it down rather than leave a plausible-sounding condition
in place.

If that is unsatisfying, the half worth doing early is the metric alone. It is
small, it is the part that catches the #10 condition, and it converts this entry
from "decide to look" into a real trigger.

Full reasoning on the issue.

### relay poison-record dead letters carry an empty tenant key

**Issue:** [#25](https://github.com/lilabrooks/my-local-platform/issues/25) ·
**Found:** 2026-08-25 · **Address:** when someone first reads the DLQ to
diagnose something

The undecodable-record path builds a dead letter with a zero `Record`, and
`deadLetter` keys the message by `dl.Record.TenantID` — which is `""`. Every
poison record keys identically and none is attributable to a tenant.

No practical impact today, because the DLQ has one partition. It becomes
confusing at exactly the moment someone needs it to be clear, which is why the
trigger is an event rather than a date.

Done means the source partition and offset carried as fields rather than only
inside the reason string, poison records keyed by something meaningful or a note
explaining why an empty key is right for them, and the raw bytes of the
undecodable record preserved so it can actually be diagnosed.

### relay delivery is not cancelled when the consumer group rebalances

**Issue:** [#69](https://github.com/lilabrooks/my-local-platform/issues/69) ·
**Found:** 2026-08-30 · **Address:** on a change to `RELAY_DELIVERY_TIMEOUT` or
`RELAY_RETRY_DELAYS` that lets one record's total work approach the 30s stall
budget (`config.DefaultStallBudget`)

`runDeliver` builds one context from `signal.NotifyContext`
(`cmd/relay/main.go:174`) and passes it down through `Consumer.Run` and
`handle` into `Deliver`. Nothing cancels it when the group changes generation —
kafka-go does joins and generation changes on background goroutines that never
touch a caller's context. So an old partition owner can still be mid-POST after
the partition has moved.

This entry is the *mechanism*, not the whole of what
[#54](https://github.com/lilabrooks/my-local-platform/issues/54) covered. That
issue's scheduled items are done and it is closed — the ordering tests exist and
are gated, `deliver.go`'s comment no longer claims the context is cancelled on
rebalance, and ADR 0006's Verification section records both results. #69 was
split out for the part nobody is scheduled to fix, which is this one: the
decision not to make delivery rebalance-aware, and why that is defensible.

It was pointed at #54 until this file was reconciled on 2026-08-31 and #54 had
closed underneath it. Every entry here names an **open** issue; one pointing at
a closed issue reads as resolved-but-not-removed, which is the state this file
says it does not keep.

**The predicted consequence did not reproduce.**
`scripts/verify-ordering-rebalance.sh` was written to catch it: two runs, both
with a real handover (12 partitions split 6/6) and a redelivered record, and no
ordering violation in either. Stopped at two rather than tuning until a positive
appeared, which would have been sampling to a conclusion. Duplicates are not
violations — at-least-once is the stated contract and ADR 0006:225 accepts them.

**Why the window is hard to hit, and why that is the trigger.** A single attempt
is capped by `RELAY_DELIVERY_TIMEOUT` while joining a group takes seconds, so
the old owner's in-flight attempt tends to finish before the new owner resumes —
and a late arrival landing before the next record is not a reordering. The case
that would bite is an old owner whose work on one record spans the whole
rebalance, which needs a longer timeout or a longer retry schedule.

That is a real trigger rather than a hopeful one, and it is the distinction
[#21](https://github.com/lilabrooks/my-local-platform/issues/21)'s entry above
had to learn twice: it is a deliberate config change, and relay's startup check
already rejects schedules whose worst case passes the stall budget. Someone
raising either value is the event, and the existing guard is where it surfaces.

**Corrected 2026-08-31.** This paragraph used to end "`ValidateLiveness` bounds
more than the liveness it is named for", and the trigger named
`DefaultRebalanceTimeout`. Both assumed the cap bounded consumer-group
liveness. It never did — kafka-go's group management runs independently of how
long a handler takes, so a consumer asleep in a retry does not miss the rejoin.
The cap is real but its basis is head-of-line stall and the pod's termination
grace period, recorded in
[ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md). The trigger survives the
correction; only its unit changed.

**This bound is a hypothesis, not a measurement.** Two clean runs do not
establish that ordering cannot break here, and nothing in the repository
currently measures whether an old owner ever completes a delivery past the new
owner's offset. Done means either instrumenting the handover to answer that, or
cancelling delivery on generation change and removing the question.

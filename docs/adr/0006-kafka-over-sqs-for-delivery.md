# 6. Kafka rather than SNS to SQS for webhook delivery

Date: 2026-08-24
Status: Accepted

## Context

`relay`, the first application on this platform, is a webhook delivery service:
tenants POST events to it, and it delivers them to subscriber URLs with retry,
backoff and a dead-letter path. See [the goal](../goal-relay.md) and
[the roadmap](../roadmap-relay.md).

Something has to sit between accepting an event and delivering it. This
repository already contains two candidates, both working:

- Apache Kafka in KRaft mode, local and free ([ADR 0004](0004-real-kafka-not-emulated.md)).
- An SNS topic fanning out to an SQS queue with a DLQ and a queue policy, built
  by Terraform at `infra/terraform/envs/dev/main.tf:111-163`, and emulated
  locally by floci ([ADR 0002](0002-floci-over-localstack.md)).

The decision must be made now because M1 of the roadmap writes the producer, and
the producer is the part that encodes the choice.

## Decision drivers

The first two drivers decided this. The rest are why the decision is also
defensible on its merits, which is a different claim and is recorded separately
so a reader can tell them apart.

**Deciding:**

1. **Working with Kafka is the goal.** This repository exists to learn and
   demonstrate a distributed-systems stack, and Kafka is the thing being
   learned. [ADR 0004](0004-real-kafka-not-emulated.md) already made this
   reasoning explicit when it rejected emulated MSK: "the goal is learning
   Kafka, not learning MSK's control plane." The same logic applies one layer
   up.
2. **Cost is out of scope.** The AWS deployment is ephemeral by design --
   minutes to hours, a demo, a screen capture, then `terraform destroy`. MSK's
   ~$547/month floor is never paid. A cost comparison against SQS would be
   comparing a number this project incurs against one it does not.

**Supporting, and genuinely load-bearing at this scope:**

- **Driver 3, replay.** Redelivering events a subscriber missed is a core
  webhook product feature. Stripe retries for 3 days; Shopify for 48 hours.
- **Driver 4, independent consumers added after the fact** -- delivery, audit
  and analytics reading the same history, each at its own position.
- **Driver 5, lag as a scaling signal** for M2's KEDA work.

**Checked and found not to separate the options:**

- **Ordering.** SQS FIFO offers ordering per `MessageGroupId`, a genuine
  equivalent of the partition key. Its limits (300 TPS ungrouped, 3,000 batched,
  20,000 in flight) are far beyond anything `relay` will reach.
- **Scaling signal.** KEDA has an `aws-sqs-queue` scaler on queue depth. Lag is
  not uniquely Kafka's.

Recording these two matters. The easy version of this argument -- "a log beats a
queue" -- is too broad, and a reader who knows SQS FIFO would spot it.

## Options considered

### A. Kafka topic partitioned by tenant -- chosen

`mlp.relay.deliveries` keyed by tenant id, with `mlp.relay.deliveries.dlq` for
exhausted retries. Replay is an offset reset. A later audit consumer is a new
group id reading retained history. Ordering per tenant follows from the key.

### B. SNS to SQS with a DLQ

Already built in Terraform, zero idle cost, no partition planning, no consumer
group rebalancing, operated by AWS.

Loses on driver 3 decisively: **SQS deletes on acknowledgement.** Once a
delivery succeeds the message is gone, so replay needs a separate event store --
which is the thing Kafka already is. Driver 4 fails similarly: a consumer
subscribed later cannot see events that predate it.

### C. Postgres as the queue

`SELECT ... FOR UPDATE SKIP LOCKED` over a table. Postgres is already in the
`core` profile, so this adds no infrastructure, and at `relay`'s realistic
volume it would work. It is also, per the evidence below, what Segment
eventually built for this exact problem.

Rejected on driver 1: it does not involve Kafka, which is the point. Worth
stating plainly that a cost-optimised low-volume production service should
probably pick this.

### D. RabbitMQ

In the `messaging` profile. Good routing, per-queue ordering. Rejected on driver
3 for the same reason as SQS: messages leave the broker on acknowledgement.

## Decision

Back `relay` delivery with Kafka, option A.

The decision is driven by drivers 1 and 2 -- this is a project for working with
Kafka, and the cost that would argue against it is never incurred. Drivers 3
through 5 mean the choice is also sound on its merits at this scope rather than
merely permitted.

The SNS to SQS resources stay in Terraform. Running `relay` against both and
writing up where each breaks down is optional later work, and that comparison
needs both alive.

## Consequences

**What this makes easier.** Replay becomes an offset reset. An audit consumer
becomes a new group id. Per-tenant ordering follows from the key. Consumer lag
becomes the scaling signal M2 needs, and it is the honest signal -- a consumer
blocked on a slow HTTP call burns no CPU, so an HPA on CPU cannot see the
backlog at all.

**Partition count becomes a hard ceiling** on consumer parallelism, fixed early
and awkward to change: `--alter --partitions` only increases, and increasing
reshuffles the key-to-partition mapping, breaking ordering for records already
on the log. The roadmap sets 12 partitions in M0 for this reason.

### Head-of-line blocking, and the scale at which it becomes fatal

One event may have several subscribers. The MVP handles all of an event's
subscribers before committing the offset, so one slow subscriber delays every
other subscriber on that partition, and cross-tenant isolation holds only to the
degree tenants hash to different partitions.

This is not a caveat invented here. It is the documented failure mode of this
design, and the canonical writeup is Segment's
[Centrifuge post](https://segment.com/blog/introducing-centrifuge) (May 2018,
now hosted by Twilio). Segment was already running Kafka at roughly 1M
messages/sec and still built something else for the delivery layer, after
working through the same architectures in order:

| Their architecture | What broke |
|---|---|
| Single shared queue | One slow endpoint backpressures everything. With 200+ endpoints at 99.9% availability each, an hour-long pipeline outage roughly once a day |
| Queue per destination | Fixes endpoint isolation, but one large customer's contiguous batch blocks every other tenant behind it |
| Queue per (source, destination) -- the ideal | 42,000 sources averaging 2.1 destinations each = 88,000 queues. No log or queue system supports that cardinality at acceptable cost |

They ended up storing jobs as immutable rows in MySQL, so delivery order became
a SQL query rather than a physical position in a log. Their framing of the crux
is the sharpest statement of the problem: a queue supports push and pop and
nothing else, and delivery order is fixed by the producer at write time, so
reordering around a failure means rewriting the data.

**The variable that decides this is not throughput. It is the cardinality of
isolation domains** -- how many `(tenant, subscriber)` pairs must be able to
fail independently. Kafka gives exactly as many as there are partitions.

For `relay` that is 12, against a handful of demo tenants, so the design is
comfortably inside its envelope. It becomes wrong as concurrently-degraded
`(tenant, subscriber)` pairs approach the partition count -- and past that point
no configuration fixes it, only a different primitive. The intermediate fix, a
second topic keyed by `(tenant, subscriber)` so each delivery is its own record,
buys headroom but does not change the shape of the limit.

Worth being explicit: Svix and Convoy, the two dominant webhook gateways, both
use queues rather than logs for delivery. Kafka-backed webhook delivery does
exist in production at scale -- Toss Payments reportedly runs hundreds of
millions of webhooks a day on one, with a Redis idempotency cache and dashboard
replay -- but that is a secondhand report and should be treated as such.

### Retry duration is bounded by consumer group liveness

A delivery consumer that sleeps between retries holds its partition assignment
for the whole wait. So **in-process retry cannot span Svix's published schedule**
-- eight attempts across 27h35m5s -- and the production preset has to be
rejected rather than merely discouraged.

An earlier version of this section named the wrong mechanism. It said Kafka
evicts a consumer that does not poll within `max.poll.interval.ms`, five minutes
by default. That is the Java client's design, and this service uses
`segmentio/kafka-go`, which has no such setting: a background goroutine
heartbeats every `HeartbeatInterval` (3s) independently of what the application
is doing, so a sleeping consumer is **never dropped for being slow**. Caught
while implementing the startup check that this paragraph justifies.

What actually goes wrong is worse in one way and narrower in another:

- **Nothing else on that partition moves for the entire wait.** The consumer is
  alive, heartbeating, and holding the assignment. One subscriber that is down
  stalls every tenant hashing to the same partition -- head-of-line blocking
  again, on a scale set by the retry budget rather than by one slow request.
- **A rebalance during the wait is not survivable.** The coordinator gives
  members `RebalanceTimeout` (30s by default in kafka-go) to rejoin. A consumer
  asleep in a retry misses it, its partitions are reassigned, and the delivery
  is redelivered elsewhere. KEDA scaling makes rebalances routine rather than
  rare, which is precisely the M2 demo.

So the bound is the rebalance timeout, not a poll interval, and it is 30s rather
than 5 minutes.

This remains a genuine asymmetry with option B that the driver list above
missed. SQS visibility timeout extends to 12 hours, so a queue absorbs long
retries without a second mechanism. On a log, the record must be parked and the
offset committed. Two ways to do that:

- **Tiered retry topics** -- a 5s topic, a 5m topic, a 30m topic, each with a
  consumer whose sleep fits inside its own rebalance window. The conventional
  Kafka answer.
- **A due-at row in Postgres with a scheduler**, which is Centrifuge again,
  arrived at from the opposite direction.

Neither is MVP work. What M1 owes is a config surface that cannot lie: a
schedule whose total reaches the rebalance timeout is **rejected at startup**,
not discovered through reassignment under load. `demo` totals 15s against a 30s
default and passes; `standard` does not, and enabling it is what forces the
mechanism above.

**Files that change together.** `local/bootstrap/kafka-topics.sh` (topics),
`services/relay` (producer and consumer), `services/smoke` (the round-trip
check), and in M2 the KEDA `ScaledObject`, whose trigger type is coupled to
this choice.

## Failure semantics

| Failure | Behaviour | Why |
|---|---|---|
| Broker unreachable at ingest | Return 503; the tenant retries | Accepting into memory and dropping is the one unrecoverable outcome |
| Delivery returns 5xx | Retry on the Standard Webhooks schedule, offset uncommitted | At-least-once |
| Retry budget exhausted | Produce to the DLQ with the failure reason, then commit | The event is parked, not lost |
| DLQ produce itself fails | Do not commit; the record is redelivered | A duplicate delivery beats a silent loss |
| Consumer dies after delivering, before commit | The subscriber receives a duplicate | Accepted; every delivery carries a stable `webhook-id` so subscribers can dedupe |
| One subscriber of several fails | Only that subscriber's delivery is dead-lettered; the offset commits once all subscribers reach a terminal state | Partial success must not replay the successful deliveries |

The contract offered to subscribers is **at-least-once with a stable delivery
id**, not exactly-once. Saying so in the tenant-facing docs is part of the work,
because a subscriber assuming exactly-once will eventually double-charge
someone.

## Verification

Sources checked on 2026-08-24: MSK and SQS pricing against
<https://aws.amazon.com/msk/pricing/> and two independent calculators; SQS FIFO
quotas against AWS published limits; KEDA's `aws-sqs-queue` and `apache-kafka`
scalers against KEDA documentation for 2.12 through 2.21; Segment's figures
against their Centrifuge post; Stripe and Shopify retry windows against their
published webhook documentation.

### Replay, the load-bearing supporting claim -- measured 2026-08-25

Driver 3 is the only technical argument that separates this from option B, so
it is the one that had to be executed rather than described. It was not, through
the whole of M1, which is why it is recorded here in detail.

```bash
make up-apps && make seed
make relay-replay-verify
```

The check posts events, waits for delivery, **wipes the sink's record of them**
-- so a redelivery cannot be confused with a memory of the first one -- moves
the consumer group back in time, and asserts the same `webhook-id`s arrive
again.

```text
PASS delivered 3 events
==> resetting relay-deliver to 2026-08-25T14:13:16.000 UTC (last 2m)
    partition 7   -> offset 8
    (11 other partitions -> offset 0)
==> starting relay-deliver
PASS every event was delivered again after an offset reset

  events posted and redelivered: 3
  total delivered in the window:  9
```

It runs in CI, so the argument is guarded rather than asserted.

Two things the exercise taught that the ADR had not anticipated:

- **Kafka refuses to move a group's offsets while it has members** --
  "Assignments can only be reset if the group is inactive". Replay therefore
  costs a consumer restart, which is a real operational property and not
  obvious from the design. `scripts/relay-replay.sh` handles it rather than
  leaving it as a trap.
- **A replay redelivers the whole window, not a selected event.** Nine
  deliveries for three events under test. That is correct for "resend
  everything from the last six hours" and wrong for "resend this one", which
  would need a different mechanism.

Driver 3 holds. Replay cost one shell script and no application code, because
the log already had the events -- which is the thing option B could not offer.

### Ordering, run 2026-08-30

**Per-tenant ordering holds in the steady state, and is now gated on every
push.** `scripts/verify-ordering.sh` posts 40 events for one tenant one at a
time, so the accepted order is known, and compares it against the order the sink
recorded them in. It uses `globex` rather than `acme`: acme's second
subscription points at `/hooks/flaky`, and since the offset commits only once
every subscriber reaches a terminal state, each record would wait out the retry
budget and the script would be measuring the retry schedule.

The assertion was checked against a deliberately reversed expectation before the
pass was believed — it fails, names the first divergence, and exits 1.

**Across a rebalance, the answer is weaker: two runs, no violation, and no proof
it cannot happen.** `scripts/verify-ordering-rebalance.sh` adds a second
consumer mid-run and waits for the broker to report the group has changed
generation. Both runs saw a real handover (12 partitions split 6/6) and one
redelivered record each, so the interrupted path was exercised rather than
skipped. Neither produced a delivered sequence that went backwards once
consecutive duplicates were collapsed.

Stopped at two runs deliberately. Continuing until a positive case appeared
would be sampling to a conclusion rather than a result.

Why the window is hard to hit is a **hypothesis this repository does not
measure**: `RELAY_DELIVERY_TIMEOUT` caps one attempt while joining a group takes
seconds, so an old owner's in-flight attempt tends to finish before the new
owner resumes. If that holds, `config.ValidateLiveness` bounds more than the
liveness it is named for. Settling it needs instrumentation at the handover.
The mechanism behind the question — delivery is not cancelled on a generation
change — is recorded in [the backlog](../backlog.md) with the condition that
would make it matter, and in
[#54](https://github.com/lilabrooks/my-local-platform/issues/54).

### Duplicate on crash, run 2026-08-31

**A consumer killed between delivering and committing redelivers the same
`webhook-id`.** `scripts/verify-duplicate-on-crash.sh` asserts it, and CI runs
it on every push.

The failure-semantics table above states this as settled — "the subscriber
receives a duplicate … Accepted; every delivery carries a stable `webhook-id`
so subscribers can dedupe" — and nothing had executed it. It is also the half
of the at-least-once contract that cannot be checked by watching relay work,
because it is a claim about what happens when relay stops working.

```text
delivered as evt_e5faae93863123716990d04b65df698f
SIGKILL, restart
webhook-id evt_e5faae93863123716990d04b65df698f delivered 2 times
distinct webhook-ids for this run: 1
```

The stability of the id is asserted separately from the redelivery. A fresh id
per attempt would still be "at least once" and would be useless: the subscriber
side of this contract is deduping on `webhook-id`, which is target behaviour 4
in [goal-relay.md](../goal-relay.md).

**Why `acme` and not a single-subscriber tenant.** The offset commits only once
every subscriber for a record reaches a terminal state, so acme's second
subscription — `/hooks/flaky`, which always fails — holds the commit open for
seconds while `/hooks/ok` has already been delivered. That gap is the window the
kill has to land in.

Verified by running it against `globex`, which has one healthy subscriber: the
commit follows delivery too closely to interrupt, no duplicate appears, and the
check fails. The negative case is what shows the assertion is doing work.

Also measured, and worth knowing before relying on it: compose's
`restart: unless-stopped` does **not** bring the container back from a SIGKILL —
it stayed `Exited (137)`. It restarts the exit-1 case it was added for, a fatal
broker fetch. The script restarts the consumer explicitly, which is also the
more faithful model, since what revives a crashed consumer here is Kubernetes
rather than the container runtime.

### Still planned

- **Head-of-line blocking**: degrade one subscriber and measure delivery delay
  for an unrelated tenant on the same partition. This should reproduce Segment's
  first architecture in miniature, and demonstrating it deliberately is more
  useful than avoiding it.

Dead-lettering is covered: the `relay` smoke check asserts a failed delivery
reaches `mlp.relay.deliveries.dlq` with a reason on every `make smoke`.

## Rollback

Before M1 ships, reverting is deleting `services/relay`. After it ships,
switching to option B means rewriting the producer and consumer and building an
event store for replay -- roughly the whole service.

## Revisit when

- Concurrently-degraded `(tenant, subscriber)` pairs approach the partition
  count. First move is the per-subscriber topic; past that it is a different
  primitive, as Segment found.
- `relay` is ever considered for real production use at low volume, where SQS
  FIFO or option C would be cheaper and simpler.
- Replay turns out to be awkward enough in practice that it stops justifying the
  operational cost.
- The project's goal stops being "work with Kafka." Driver 1 is the load-bearing
  one, and it is a goal rather than a technical property.

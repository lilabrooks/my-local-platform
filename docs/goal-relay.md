# Goal: `relay`

Date: 2026-08-24 · Last audited: 2026-09-03
Status: **Built.** The target behaviour below was delivered by M1 and M2, and
the demo it describes runs as `make relay-demo`. Kept as written rather than
rewritten in the past tense: it is the thing the work was measured against, and
a goal edited to match its outcome cannot be used to judge one.

## What it is

`relay` is a webhook delivery service. Tenants POST events to it; it durably
buffers them in a Kafka topic partitioned by tenant; a consumer group delivers
them to subscriber URLs with retry, backoff, and a dead-letter path for
deliveries that exhaust their budget.

## The value proposition

**Delivering a webhook is easy. Delivering the one that failed at 3am, after the
subscriber came back, in the right order, without double-charging anyone, is the
entire product.**

That is the part of the problem worth building, and it is why a log is a sound
substrate for it. A queue deletes a message when it is acknowledged, so the
moment a delivery succeeds the evidence is gone -- and "resend everything from
the last six hours" becomes a feature needing a second system to support. A
partitioned log already is that system.

The *deciding* driver was simpler: this project exists to work with Kafka, and
its AWS footprint is ephemeral enough that cost never enters.
[ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md) records both, kept apart, so
the technical case is not dressed up as the reason. It also records the two
drivers where SQS genuinely wins and the scale at which this design stops
working.

This matters beyond the demo. Stripe, GitHub and Shopify all run a service
shaped like this one, and all three ship replay, because every team consuming
webhooks eventually has an outage and asks for the events they missed.

### What it proves

`relay` was chosen over four other candidate applications because **consumer lag
is intrinsic to its domain**. A subscriber that takes two seconds to answer, or
returns 503 for ten minutes, produces genuine backpressure on a real consumer
group. Every other candidate needed synthetic load to make lag appear, which
turns the autoscaling story into theatre.

So the thing `relay` demonstrates is not "events move through Kafka." It is:

- **Backpressure made visible and actionable.** Lag is the signal, KEDA is the
  actuator, and the loop closes without a human. A CPU-based HPA cannot see this
  at all -- a consumer blocked on a slow HTTP call burns no CPU while the
  backlog grows.
- **Delivery semantics stated rather than assumed.** At-least-once with a stable
  delivery id, ordering per tenant, and a documented head-of-line limitation.
  Naming what a system does *not* guarantee is the harder half.
- **Cost reasoning as a design input.** MSK costs five times EKS and bills for
  merely existing, which is why every milestone before the last runs free on
  docker-compose and minikube.

### What it teaches

The failure modes worth having are the ones that only appear when you build the
thing. Partition count as a hard parallelism ceiling. Rebalancing pauses during
a scale-up. The gap between committing an offset and completing a side effect,
which is where duplicate deliveries are born. Choosing a retry budget without a
principle for it. None of these read as real until a broker is in front of you.

### Why it can be trusted without trusting the agent

This repository is built with AI agents, and that is a stated goal rather than
an implementation detail. It also creates a specific problem: **agent-produced
work is cheap to generate and expensive to verify.** The characteristic failure
is not broken code -- it is plausible, confident, well-structured work that was
never actually executed.

The conventions here exist to close that gap, and they predate `relay`:

| Convention | What it converts |
|---|---|
| ADRs with a Verification section naming the command | "this design is sound" into "run this" |
| Smoke checks that write and read back | "the port is open" into "the round trip works" |
| `k8s/validate` manifest invariants | "the YAML looks right" into a failing test |
| Pinned image and module versions | "it worked once" into "it works the same way tomorrow" |
| Line-level citations in docs | a claim into a location a reviewer can open |
| A backlog recording *why* work was deferred | a silent gap into a decision |

`relay` is the first artifact that exercises all six at once, across a whole
service rather than a config file. Its third value proposition is therefore:
**a reviewer holding only this repository, with none of the session that
produced it, can check every claim it makes.** That is the property that makes
agent-built infrastructure worth showing to anyone.

## Target behaviour

Concrete enough to test. These are the statements M1 and M2 have to make true.

1. A tenant POSTs an event and receives `202` with an event id, or a `503` if
   the broker is unreachable. It never receives a success for an event that was
   not durably written.
2. Every accepted event reaches every active subscriber **at least once**, or
   lands in `mlp.relay.deliveries.dlq` with a recorded reason once its retry
   budget is spent.
3. Events for a single tenant are delivered in the order they were accepted.
4. Every delivery carries a stable id, so a subscriber can dedupe. The contract
   is at-least-once, and the tenant-facing docs must say so -- a subscriber
   assuming exactly-once will eventually double-charge someone.
5. A subscriber that is failing does not prevent delivery to a healthy
   subscriber of the same event.
6. Sustained lag on the delivery consumer group causes more consumer pods to
   exist, and draining it causes fewer.

**Known limitation, stated up front:** point 5 holds per event, not per
partition. The MVP commits an offset only after all of an event's subscribers
reach a terminal state, so one slow subscriber delays others on the same
partition, and cross-tenant isolation holds only to the degree tenants hash
apart. The fix is a per-subscriber topic; it is deferred deliberately and
recorded in ADR 0006 rather than discovered later.

This is the documented failure mode of the design, not a surprise. Segment hit
it at scale, worked through the same architectures in order, and ended up
replacing the queue entirely. ADR 0006 records their numbers and the point at
which `relay`'s version stops working -- which is well above anything this
project will reach, and worth demonstrating on purpose rather than avoiding.

## Ingest idempotency contract

A non-empty `idempotency_key` is scoped to one tenant. Repeating the same key,
event type and JSON data returns `202` with the original relay event id and
writes no new Kafka record. Reusing the tenant and key with different type or
data returns `409 Conflict`. Another tenant may use the same key independently.
Missing, empty and whitespace-only keys create a new event on every request.

Relay commits the event row before writing to Kafka and records `published_at`
only after the producer reports success. Concurrent requests lock that row, so
one request publishes while the others wait and then reuse its result. A
producer error returns `503` and leaves the row pending. Repeating the same
request retries publication with the stored event id.

Kafka acknowledgements can fail ambiguously. Recovery may therefore put the
same event id on Kafka twice if the first write reached the broker before the
error. Subscriber delivery remains at least once, and the stable `webhook-id`
is the deduplication key.

## The delivery contract

`relay` implements [Standard Webhooks](https://www.standardwebhooks.com/) rather
than inventing its own headers and retry schedule. The specification launched in
April 2023, Svix-led and co-signed by Zapier, Twilio, Lob, Ngrok, Vercel and
Supabase.

| Element | Value |
|---|---|
| Headers | `webhook-id`, `webhook-timestamp`, `webhook-signature` |
| Signature | HMAC-SHA256, base64-encoded, `v1,` prefix |
| Timestamp tolerance | 5 minutes, for replay protection |
| Payload | JSON, `type` plus `data` envelope |
| Retry schedule | 8 attempts: immediate, then 5s, 5m, 30m, 2h, 5h, 10h, 10h -- 27h35m5s total |
| Idempotency key | `webhook-id`, reused by the subscriber |

This closes what was an open question here: what retry budget is defensible, and
on what principle. Adopting a published schedule is a better answer than a
plausible-looking one invented on the spot, and it makes the contract checkable
against something outside this repository -- the same property every other
convention here aims for.

For contrast, and to show the range of defensible answers: Stripe retries for
3 days; Shopify retries for 48 hours and then **deletes the webhook
subscription entirely**, which a subscriber recovering slowly from an outage
would not enjoy.

### The schedule is configuration, never a constant

The spec's budget is right for production and useless for a demo -- reaching the
DLQ has to take seconds when someone is watching. So the schedule is a list of
delays chosen at startup, from the first line of code rather than retrofitted
once the backoff logic is written and tested.

| Setting | Meaning |
|---|---|
| `RELAY_RETRY_DELAYS` | Explicit comma-separated durations, e.g. `1s,2s,4s,8s` |
| `standard` preset | Svix's published schedule: `5s,5m,30m,2h,5h,10h,10h` |
| `demo` preset | `1s,2s,4s,8s` -- dead-letters in 15 seconds |
| `RELAY_RETRY_JITTER` | On by default; off makes tests deterministic |

Four rules that keep the surface honest:

1. **The list length is the attempt budget.** There is no separate
   max-attempts setting, because two knobs that can contradict each other are a
   defect waiting to be filed.
2. **The total is logged at startup**, so the longest an event can sit before
   dead-lettering is a fact an operator can read rather than compute.
3. **Invalid schedules fail at startup** -- empty, negative, or unparseable.
4. **A schedule longer than the consumer can survive is also rejected at
   startup.** A consumer sleeping between retries holds its partitions for the
   whole wait, so nothing else on that partition moves — and a rebalance during
   the wait reassigns the delivery, because the coordinator only allows
   `RebalanceTimeout` (30s in `segmentio/kafka-go`) to rejoin. KEDA scaling
   makes rebalances routine, so this is not a rare case.

   `demo` totals 15s and passes. `standard` totals 27h35m5s and does not;
   running it needs the parking mechanism in
   [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md#retry-duration-is-bounded-by-how-long-one-member-may-stall).
   Failing at startup beats losing the delivery mid-retry.

> **Mechanism correction, 2026-08-31.** The startup rejection above shipped,
> but the rebalance explanation did not survive implementation. kafka-go keeps
> group membership alive on background goroutines while a handler waits. The
> 30s cap instead bounds how long one member may stall every partition it owns
> and fits the record drain inside Kubernetes' shutdown grace period. ADR 0006
> records the code paths and measured handover. The original target text stays
> here because this document is the prediction the work was judged against.

## The demo

The pacing goal is to reach something demonstrable quickly, so the demo is the
deliverable, not a byproduct. Target: **three minutes, scripted as
`make relay-demo`.**

1. Stack up. `curl` an event in; it appears at the sink immediately.
2. Slow the sink to two seconds and open the tap. Lag climbs on a Grafana panel.
3. KEDA scales the delivery consumer from 1 pod to several. Lag drains.
4. Release the sink. Pods scale back down.
5. Point a second subscriber at a sink that always fails. That delivery lands in
   the DLQ with a reason, visible in Kafka UI, while the healthy subscriber is
   unaffected.
6. `make relay-replay SINCE=10m`. Everything in the window is delivered again --
   the thing a queue could not have done. `make relay-replay-verify` asserts it,
   and CI runs that on every push.

Steps 2 through 4 are the part that is hard to fake, and step 6 is the part that
justifies [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md).

## How we will know it works

```bash
make lint && make test        # linters and unit tests across all modules
make up && make seed && make smoke   # PASS relay on the round-trip check
make k8s-validate             # manifest invariants, including relay
make relay-demo               # the scripted demo above, end to end
```

A claim not covered by one of these is a claim this project has not earned.

## Non-goals

- **Exactly-once delivery.** Not offered, and saying so is part of the design.
- **A tenant-facing UI or auth system.** Tenants are rows in a table.
- **Running in production.** `relay` is a demonstration and a learning vehicle.
  If that ever changes, ADR 0006's cost analysis needs revisiting first -- at
  low volume, SQS FIFO or Postgres would likely be cheaper and simpler.
- **Competing with a real webhook product** on features. Replay, retry, DLQ and
  ordering are the interesting quarter of one.
- **Authenticating anyone.** `tenant_id` is an unvalidated string in the request
  body, so whoever can reach ingest can publish as any tenant. Per-tenant
  ordering is real; per-tenant *isolation* is nominal until a caller has to
  prove who it is. Worth stating because ordering-per-tenant is a headline
  property and the tenant is currently self-asserted.
- **High throughput.** Correctness and observable behaviour under backpressure
  matter here; records per second do not.

## Constraints

- Everything through the KEDA demo must run free on docker-compose and minikube.
  Real AWS stays opt-in, ephemeral, and behind a Terraform flag defaulting to
  `false` ([ADR 0001](adr/0001-local-first-with-ephemeral-aws.md)).
- Memory is a real budget. The stack is already ~1.6 GB, and minikube adds
  ~1.8 GB. `relay` and its sink need to be small.
- Every image and module version pinned.
- New manifests must satisfy `k8s/validate` rather than requiring it to relax.

## Open questions

- Whether `segmentio/kafka-go` composes cleanly with the MSK IAM token signer,
  which decides the client for the whole service. Resolved in M0, before any
  producer code exists.
- How pods in minikube reach the compose broker. Three viable answers, none
  chosen yet; see the roadmap's M2 risk.
Resolved while building ingest: **the record and the subscriber body are
deliberately different shapes.** The record carries routing metadata the
delivery consumer needs — tenant, event id, when it happened — and none of that
belongs in a subscriber's request body. Coupling them would make the log's
schema an external contract, so every field the consumer ever needs would become
a field subscribers can see and depend on. `event.Record` is internal and may
grow; `event.Payload` is exactly the specification's `type` plus `data`, with a
test asserting it carries nothing else.

---

Sequencing and milestones are in [the roadmap](roadmap-relay.md). The choice of
Kafka is in [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md). M0, M1 and M2
are built and their milestones closed; M3 and M4 remain proposed.

This sentence used to read "Nothing in either has been built yet." It was true
on 2026-08-24 and survived three milestones sitting directly under a header
saying **Built** — the same drift the 2026-08-27 audit found six of, caught here
by an external review rather than by anyone re-reading the file.

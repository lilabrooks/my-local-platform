# Roadmap: `relay`, the first application

Date: 2026-08-24 · Last audited: 2026-09-05
Status: **M0 through M3 are built. M3's whole-application proof passed on
2026-09-05, and [#90](https://github.com/lilabrooks/my-local-platform/issues/90)
is closed.** On 2026-09-01, the owner selected M3 followed by M4. The satisfied
M3 gate released M4 contract work at
[#91](https://github.com/lilabrooks/my-local-platform/issues/91). Earlier
versions called M3 and M4 optional alternatives; that status ended with the
owner decision.

`relay` is a webhook delivery service: tenants POST events to it, it durably
buffers them in Kafka partitioned by tenant, and a consumer group delivers them
to subscriber URLs with retry, backoff and a dead-letter path.

- **Why it exists and what it is worth:** [goal-relay.md](goal-relay.md)
- **Why Kafka and not SNS to SQS:** [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md)

The reason to build this rather than a generic order pipeline is that
**consumer lag is intrinsic to the domain**. A subscriber that takes two seconds
to answer, or returns 503 for ten minutes, produces genuine backpressure on a
real consumer group. Every other candidate needs synthetic load to make lag
appear, which makes the autoscaling story a demo rather than a demonstration.

## Sequencing principle: reach the demo, then decide

The path to something demonstrable was **M0 to M1 to M2**, and the demo is the
KEDA scaling loop described in [goal-relay.md](goal-relay.md#the-demo). M2 was
the planned decision point; work after it required an owner choice.

The owner made that choice on 2026-09-01: finish M3 locally, then use its
whole-application proof as the gate for M4.

This means one deliberate inversion: the observability work is split. The parts
the demo needs -- the lag metric and one Grafana panel -- move forward into M2.
Attempt history, tracing and idempotency move behind it into M3. Building the
service properly and then demonstrating it would be the natural order; it is not
the fast one.

## The cost shape that governs the sequencing

| Component | Rate | If left running |
|---|---|---|
| EKS control plane | $0.10/hr | ~$73/mo, ~$110 with nodes and NAT |
| MSK Serverless | $0.75/cluster-hr + $0.0015/partition-hr + $0.10/GB in, $0.05/GB out | **~$547/mo in cluster-hours alone** |
| MSK Provisioned | 3-broker HA minimum, 3x kafka.m5.large @ $0.21/hr | ~$460/mo before storage |

Rates checked 2026-08-24 against <https://aws.amazon.com/msk/pricing/> and two
independent calculators. Verify again before M4 -- both schedules change.

MSK bills for existing, and it is five times EKS. Hourly, though, the full M4
stack -- MSK Serverless, EKS control plane, two nodes, one NAT -- is roughly
**$1/hr**. A four-hour session costs about four dollars.

So M0 through M3 run free on docker-compose and minikube, and M4 is a single
measured session ending in `terraform destroy`. This is
[ADR 0001](adr/0001-local-first-with-ephemeral-aws.md) applied to a new layer,
not a new policy.

## Topic layout

`relay` gets its own topics rather than reusing `mlp.events`:

| Topic | Partitions | Purpose |
|---|---|---|
| `mlp.relay.deliveries` | 12 | one record per event, keyed by tenant id |
| `mlp.relay.deliveries.dlq` | 1 | deliveries that exhausted their retry budget |

`mlp.events` is the platform smoke topic, and an application writing into the
topic a health check consumes from mixes concerns that should stay separate. It
also keeps `mlp.events` small, which mattered for
[#1](https://github.com/lilabrooks/my-local-platform/issues/1).

Twelve partitions rather than three because **partition count is the hard
ceiling on consumer parallelism** -- KEDA will not scale a consumer group past
it, since the extra pods would sit idle. Three pods is a thin demonstration.

This must be set in M0, before there is data. `kafka-topics.sh --alter
--partitions` can only increase, never decrease, and increasing reshuffles the
key-to-partition mapping, which breaks per-tenant ordering for records already
on the log.

---

## M0 -- Clear the runway

**Free. Half a day. No application code.**

1. **Fix the smoke Kafka check**
   ([#1](https://github.com/lilabrooks/my-local-platform/issues/1)).
   `services/smoke/internal/checks/messaging.go:44-45` reads from
   `kafka.FirstOffset` with a fresh group id, so it replays the whole topic
   before reaching its own marker. Position the reader at the end *before*
   publishing.

   Dedicated relay topics mean `mlp.events` no longer grows, so this is not
   strictly forced by the new app -- but the relay check in M1 would copy the
   same replay-from-earliest pattern. Fix it here and reuse the corrected shape.

2. **Add the relay topics** to `local/bootstrap/kafka-topics.sh`, using the same
   idempotent `topic` helper the existing two use.

3. **Settle the Kafka client. Resolved on 2026-08-24: stay on
   `segmentio/kafka-go`, and expect to write a small adapter at M4.**

   `aws/aws-msk-iam-sasl-signer-go` v1.0.4 exposes
   `signer.GenerateAuthToken(ctx, region) (string, int64, error)` -- token,
   expiry in milliseconds, error -- plus variants for a named profile, an
   assumed role, a credentials provider, and `GenerateAuthTokenFromWebIdentity`,
   which is the one IRSA needs. The library is client-agnostic; the only worked
   example in its README is Sarama, through `sarama.SASLTypeOAuth` and an
   `AccessTokenProvider`.

   kafka-go ships exactly two SASL mechanisms, `plain` and `scram` -- the whole
   contents of its `sasl/` directory. **There is no OAUTHBEARER mechanism.** So
   the two compose only through an adapter this repository would own: an
   implementation of `sasl.Mechanism` whose `Name()` returns `OAUTHBEARER` and
   whose `Start()` returns a one-shot state machine emitting the RFC 7628
   initial client response, `n,,\x01auth=Bearer <token>\x01\x01`, with the token
   from `GenerateAuthToken`. On the order of 60 lines.

   Staying on kafka-go anyway, for three reasons. It is already the client in
   `services/smoke`, so one library covers the repository. The adapter is small,
   bounded, and unit-testable with no MSK cluster involved, because the
   assertion is the initial-response bytes. And KEDA's own `apache-kafka` scaler
   is built on kafka-go and authenticates to MSK with IAM, so the combination is
   demonstrated rather than hoped for.

   Sarama is the fallback if per-connection token refresh turns out to be
   something kafka-go's interface cannot express. Switching is cheapest before
   M1 writes producers, which is why this was settled here.

**Exit: met on 2026-08-24.** The `kafka` check runs 206/199/208ms against
100,007 messages, against 211ms on an empty topic -- flat, where the target was
merely "under 15s". `make lint` 10/10 and `make test` green. Evidence and
commands in [ADR 0004](adr/0004-real-kafka-not-emulated.md#verification);
[#1](https://github.com/lilabrooks/my-local-platform/issues/1) closed.

(That line once pointed at a Resolved section in the retired `backlog.md`.
Issue #1 and its closing commit are the durable record of the fix; git history
retains the old document.)

---

## M1 -- MVP: the relay delivers

**Free. Two to three days. docker-compose only.**

The smallest thing that is genuinely a webhook service. One `curl` puts an event
in; it arrives at a subscriber; a refusing subscriber sends it to the DLQ.

### Scope

- **`services/relay`**, one Go module, one image, two roles selected by
  `RELAY_MODE`:
  - `ingest` -- `POST /v1/events` taking tenant id, event type, JSON payload and
    an idempotency key; produces to `mlp.relay.deliveries` keyed by tenant.
  - `deliver` -- consumer group `relay-deliver`; looks up subscriptions and POSTs
    to each subscriber URL following
    [Standard Webhooks](https://www.standardwebhooks.com/): `webhook-id`,
    `webhook-timestamp` and `webhook-signature` headers, HMAC-SHA256 base64 with
    a `v1,` prefix. Retries on the spec's schedule, produces to the DLQ on
    exhaustion. The contract is in
    [goal-relay.md](goal-relay.md#the-delivery-contract).

    **The retry schedule is configuration from the first commit**, not a
    constant refactored later: `RELAY_RETRY_DELAYS` with `standard` and `demo`
    presets, the list length as the attempt budget, and startup validation that
    rejects a schedule the consumer cannot survive. Full surface and the four
    rules governing it are in
    [goal-relay.md](goal-relay.md#the-schedule-is-configuration-never-a-constant).

    M2 needs the DLQ reached in seconds while someone is watching, and the
    spec's budget is 27h35m5s. Retrofitting this after the backoff logic is
    written and tested is the expensive version.
- **`services/sink`** -- a test subscriber whose latency and failure rate are
  env-configured. Not a testing hack: a slow subscriber is what makes M2 work,
  so it is a first-class component.
- **Postgres** -- one `subscriptions` table, read-only at runtime, seeded by
  `local/bootstrap/`. Attempt history is deferred to M3.
- **`/healthz` and `/readyz` on both roles** -- required by
  `k8s/validate/manifests_test.go:147` in M2, and cheap to add now.
- **Compose profile `apps`**, so relay does not load by default.
- **Smoke check `relay`** -- POST an event, poll the sink, assert the payload
  matches. Writes and reads back, per the rule in
  `services/smoke/internal/checks/runner.go`.
- **CI** -- the smoke job already brings up `core` and `messaging`; add the
  `apps` profile and an image build alongside the existing `echo` one.

### Not in the MVP

Attempt history and its query API, tracing, idempotency dedupe, dashboards,
Kubernetes, KEDA, AWS.

**Exit: met on 2026-08-25.** `make smoke` prints `PASS relay`, and CI runs it on
every push. Replay -- the deciding argument in ADR 0006 -- was found unexercised
by an audit ([#20](https://github.com/lilabrooks/my-local-platform/issues/20))
and is now covered by `make relay-replay-verify`.

The original criterion follows. `make up && make seed && make smoke` prints
`PASS relay`. A documented
run shows an event reaching the DLQ when the sink is set to fail. Unit tests
cover signature generation, the backoff schedule and the give-up decision.

---

## M2 -- The demo: KEDA scaling on lag

**Free. Three to four days. minikube.**

This is the deliverable. Kafka and Kubernetes meeting, rather than merely
coexisting.

- **`make relay-image`**, mirroring the existing `echo-image` target.
- **`k8s/manifests/relay/`** -- separate Deployments for `ingest` and `deliver`
  from one image, Service, ConfigMap, Secret for DB credentials.
- **`k8s/apps/relay.yaml`** -- ArgoCD Application. Use the real repoURL, never
  `__REPO_URL__`; the trap is documented at `k8s/apps/echo.yaml:12-19`.
- **Extend `k8s/validate`** -- the four existing invariants must pass, plus a
  fifth worth adding: a consumer Deployment's `terminationGracePeriodSeconds`
  must exceed its maximum delivery timeout, or KEDA scaling down kills
  in-flight deliveries.
- **Lag metric and one Grafana panel** -- pulled forward from the old M3,
  because the demo requires lag to be *visible*, not merely acted upon.
- **KEDA** with a `ScaledObject` on `relay-deliver` consumer lag.
- **`make relay-demo`** -- the six-step script from
  [goal-relay.md](goal-relay.md#the-demo), including the offset-reset replay
  that justifies ADR 0006.
- **Evidence** -- lag curve and pod count over time, captured for ADR 0007.

### The gap most likely to cost a day

The broker advertises `INTERNAL://kafka:19092` and `HOST://localhost:9092`
(`local/docker-compose.yml`). Neither resolves from inside minikube. Pods will
connect, receive the advertised address in metadata, and fail on the follow-up
connection -- which looks like a broker fault and is not.

Resolve it deliberately: a third advertised listener on the host's LAN address,
`host.minikube.internal`, or Kafka in-cluster for this milestone. Pick one and
write down why, because M4 changes this wiring again.

**Exit: met on 2026-08-27.** `make relay-demo` runs all six steps in 190
seconds: lag 596 with one consumer, KEDA to twelve, drained to zero, back to one.
Measurements and commands are in
[ADR 0007](adr/0007-keda-lag-autoscaling.md) and
[ADR 0008](adr/0008-in-cluster-observability-for-the-demo.md); the milestone is
closed.

Two things the milestone did not anticipate, both recorded: the demo needed
Prometheus and Grafana **in the cluster**, which amends ADR 0005
([#40](https://github.com/lilabrooks/my-local-platform/issues/40)); and
`--memory=3g` cannot run it, which is why `MINIKUBE_MEMORY` is 6g.

The original criterion follows. `make relay-demo` runs end to end. Slowing the sink drives
`relay-deliver` from 1 pod to several, lag drains, pods scale back down. Write
**ADR 0007** on lag-based autoscaling now, with measurements rather than
predictions.

---

## Decision point

M2 produced the complete autoscaling demonstration and opened the choice this
roadmap was written to reach. On 2026-09-01, the owner chose both remaining
stages in sequence.

M3 removes application uncertainty with the local brokers, database,
subscriber, and observability stack. Its whole-application proof is the
release gate for M4. M4 then runs that rehearsed application briefly on EKS,
MSK, and RDS, where the AWS control plane, IAM transport, and teardown path can
be measured. This order keeps application debugging out of the paid AWS
session.

| Stage | Why it was selected | Release condition |
|---|---|---|
| M3 | Establish the relay's semantics and operator evidence locally. | Satisfied: [#90](https://github.com/lilabrooks/my-local-platform/issues/90) closed after the whole-application proof passed. |
| M4 | Test the same application against the live AWS surfaces local infrastructure cannot reproduce. | Released: follow the dependency chain beginning with contract [#91](https://github.com/lilabrooks/my-local-platform/issues/91). |

---

## M3 -- Local relay readiness

**Complete and free. The whole-application proof passed on 2026-09-05.**

The work that turns a convincing demo into a service whose guarantees hold up
when someone reads the code carefully.

The completed scope is truthful consumer readiness and assignment evidence
([#21](https://github.com/lilabrooks/my-local-platform/issues/21)), diagnosable
poison-record dead letters
([#25](https://github.com/lilabrooks/my-local-platform/issues/25)), end-to-end
tracing ([#86](https://github.com/lilabrooks/my-local-platform/issues/86)),
delivery history ([#87](https://github.com/lilabrooks/my-local-platform/issues/87)),
ingest idempotency
([#89](https://github.com/lilabrooks/my-local-platform/issues/89)), and the
roadmap reconciliation
([#98](https://github.com/lilabrooks/my-local-platform/issues/98)). Those pieces
culminated in the whole-application local proof
([#90](https://github.com/lilabrooks/my-local-platform/issues/90)).

- **Consumer readiness and assignment evidence, built 2026-09-05.**
  `relay-deliver` stays unready until kafka-go reports its first joined group
  generation; a joined member holding 0 partitions stays ready. `relay-ingest`
  now reads group members and assignments from the broker beside lag. The
  dashboard shows process count, group members, idle members, and partitions
  without an owner. `make monitoring-ready` requires each broker series plus a
  measurement no more than 30 seconds old on every scraped ingest instance.
  Two `make relay-demo` runs produced 600 events each, raised lag to 593 and
  575, scaled from 1 consumer to 12, drained lag to 0, and returned to 1.
- **Poison-record dead letters, built 2026-09-05.** Undecodable Kafka records
  now carry source topic, partition, offset, timestamp, bounded original-key
  bytes with a full SHA-256 digest, and bounded raw bytes in the DLQ. Their
  deterministic key comes from broker coordinates rather than an inferred
  tenant. `make smoke` produced an invalid 36-byte value, read its dead letter
  back, and matched every source field and byte while the existing
  exhausted-delivery path still passed.
- **Delivery attempts persisted, built 2026-09-02.** The audit trail answers
  "did you deliver my webhook?" through `GET /v1/events/{id}/attempts`.
  `make smoke` posted one event to the real local Kafka broker, matched the
  healthy and exhausted subscribers, and read back all 4 Postgres attempt rows.
- **Idempotency, built 2026-09-03.** A non-empty tenant-and-key pair names one
  request. Identical repeats return the original event id; conflicting reuse
  returns `409`. A committed event row and row lock serialize concurrent
  requests, while `published_at` keeps a failed Kafka write from reading as a
  successful claim. `make smoke` sent 2 concurrent requests, received the same
  id twice, and found one Kafka record, one published event row, one healthy
  delivery, and 4 attempt rows.
- **Tracing, built 2026-09-03.** W3C trace context crosses HTTP, Kafka, and each
  subscriber request. `make smoke-traces` follows one event through
  `relay.ingest`, `kafka.produce`, `relay.consume`, and one
  `relay.webhook.attempt` span per persisted attempt in a single Tempo trace,
  and fails if any of them is missing. Local only: nothing under `k8s/` gives
  relay an OTLP endpoint, so the [ADR 0005](adr/0005-argocd-gitops.md) telemetry
  gap for the in-cluster case is still open and belongs to M4 with EKS and MSK.
- **Per-tenant ordering test -- built 2026-08-30.** The original M3 item was to
  prove same-tenant events land on one partition and are delivered in order.
  `make relay-verify-ordering` now runs that check on every push; the result and
  its limit across rebalances are in
  [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md#ordering-run-2026-08-30).
- **Head-of-line blocking -- measured 2026-08-31; remediation deferred.** The
  experiment confirmed that one blocked record delays every partition owned by
  its consumer member. The per-subscriber topic described in
  [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md#revisit-when) becomes work
  when concurrently degraded `(tenant, subscriber)` pairs approach the
  partition count.

### Whole-application proof, run 2026-09-05

The proof used the 12-partition compose Kafka broker and compose Postgres. The
compose phase ran relay and the controlled sink from the current source. The
cluster phase stopped those 3 app containers, kept the brokers running, and
used the existing 1-node, 6 GiB `mlp` profile with the current relay and sink
images, KEDA, Prometheus, Grafana, and ArgoCD.

```bash
make lint
make test
make k8s-validate
make up-apps
make seed
make smoke
make up-obs
make smoke-traces
make relay-replay-verify
make relay-verify-ordering
make relay-verify-graceful-drain
make relay-verify-duplicate-on-crash
make monitoring-ready
make relay-demo
make k8s-down
make up
make smoke
```

Every command passed. The traced smoke event
`evt_2f90004ddafd451a18789e98a64f794e` returned from both concurrent requests,
produced 1 Kafka record and 1 healthy delivery, persisted 1 event row and 4
attempt rows, and joined its ingest, produce, consume, and 4 attempt spans in
Tempo trace `2b74c971058f50dc7219eddfce5f5cf9`. Its poison companion preserved all
36 invalid bytes with the source topic, partition, offset, timestamp, and key.

The replay check redelivered all 3 selected events; the steady-state ordering
check delivered 40 events in accepted order. SIGTERM drained and committed an
in-flight record with the healthy delivery count staying at 1. A forced crash
redelivered `evt_19f42e88f57c22827a342e99f91e1c49` with the same webhook id,
raising its healthy delivery count from 1 to 2.

`make monitoring-ready` found fresh broker assignment evidence for every
scraped relay target. The 600-event minikube demo raised lag to 598, scaled the
group from 1 consumer to 12, drained lag to 0, and returned to 1 consumer. The
failing-subscriber and cluster replay steps then completed. Finally, the clean
branch passed the README's `make up` and `make smoke` path using Make's
`.env.example` fallback. The final smoke run ended with `all components
healthy`.

---

## M4 -- Ephemeral live AWS validation

**The owner accepted the contract in
[ADR 0010](adr/0010-live-aws-relay-contract.md) on 2026-09-05. Live AWS
validation remains unverified and requires separate owner authorization. The
fixed paid shape is estimated at $1.02/hour, capped at $1.25/hour and $5 total,
and ends in destroy.**

[#90](https://github.com/lilabrooks/my-local-platform/issues/90) closed after
the M3 proof passed, releasing M4's contract work. Its governed sequence begins
at [#91](https://github.com/lilabrooks/my-local-platform/issues/91):

1. Settle the topology, identity, cost, evidence, and teardown contract in
   [#91](https://github.com/lilabrooks/my-local-platform/issues/91).
2. Complete the three locally testable foundation items, which can proceed in
   parallel after #91:

   - add the MSK IAM transport in
     [#92](https://github.com/lilabrooks/my-local-platform/issues/92);
   - add the opt-in Terraform runtime, with every hourly resource disabled by
     default, in
     [#93](https://github.com/lilabrooks/my-local-platform/issues/93); and
   - derive relay's drain budgets from its executable shutdown sequences in
     [#101](https://github.com/lilabrooks/my-local-platform/issues/101).

3. Render the AWS relay deployment and evidence stack in
   [#94](https://github.com/lilabrooks/my-local-platform/issues/94).
4. Rehearse the deployment, demonstration, evidence capture, abort path, and
   cleanup locally in
   [#95](https://github.com/lilabrooks/my-local-platform/issues/95).
5. With separate owner authority, stage the cheap AWS tier, immutable images,
   budget protection, and reviewed plan in
   [#96](https://github.com/lilabrooks/my-local-platform/issues/96).
6. With a new owner authorization for the expensive apply, run the brief live
   validation and destroy it in
   [#97](https://github.com/lilabrooks/my-local-platform/issues/97).

Approval for #96 does not authorize #97. The low-cost staging mutation and the
hourly EKS, MSK, and RDS session are separate decisions.

ADR 0010 and the [AWS relay runbook](runbook-aws-relay.md) fix the handoff
across that sequence:

| Boundary | Contract |
|---|---|
| Topology | private MSK Serverless and RDS; EKS runs relay, internal sink, KEDA, ArgoCD, Prometheus, Grafana, and Tempo |
| Identity | EKS Pod Identity for separate ingest, deliver, and KEDA operator roles; the sink has no AWS role |
| Images | immutable `mlp-dev/relay` and `mlp-dev/sink` ECR repositories, git SHA tags, digest deployments |
| Data | 12-partition delivery topic, one-partition DLQ, two ingest replicas, deliver scales 1 to 12 |
| Exposure | EKS API restricted to the operator; workload UIs and sink reached only by `kubectl port-forward` |
| Session | three hours, destroy begins at 2 hours 30 minutes, $1.25/hour shape gate, $5 maximum |
| Terminal condition | success or failure runs state-backed destroy, service-native empty inventories, and a settled cost capture |

The three foundation issues consume #91 independently: issue #92 proves IAM
transport, issue #93 produces guarded Terraform, and issue #101 makes shutdown
execution and drain accounting share one definition. Issue #94 consumes all
three artifacts to render the workload. Issue #95 then rehearses the whole
runbook locally, issue #96 stages immutable images and the reviewed plan, and
issue #97 performs the separately authorized paid run. A change to the table
returns to the owner before it enters a plan.

The one thing the local stack structurally cannot teach: MSK Serverless supports
**IAM authentication only** -- no SASL/SCRAM -- so this is SASL_SSL with
OAUTHBEARER tokens minted from IAM credentials, delivered to pods by IRSA or EKS
Pod Identity, with `kafka-cluster:*` IAM actions as the authorization layer
instead of Kafka ACLs.

[ADR 0004](adr/0004-real-kafka-not-emulated.md) rejected emulated MSK and left
this door open: "when the goal shifts to learning MSK's control plane." This is
that shift, narrow enough to name precisely.

### Before the first apply

1. An AWS Budgets alarm for forgotten resources. The active session controls
   are the fixed resource-shape gate, the 2-hour-30-minute destroy deadline,
   the 3-hour hard stop, and the $5 approved maximum.
2. Confirm the EKS version is in **standard** support. Extended support bills
   $0.60/cluster-hour instead of $0.10, applied automatically:

   ```bash
   aws eks describe-cluster-versions --query 'clusterVersions[?status==`STANDARD_SUPPORT`].clusterVersion'
   ```

3. Create the decided ECR layout: separate immutable `mlp-dev/relay` and
   `mlp-dev/sink` repositories, exact commit tags, and digest deployments.

### Scope

- `enable_msk` variable, default `false`, alongside `enable_rds` and
  `enable_eks` in `infra/terraform/envs/dev/variables.tf`.
- MSK Serverless in the existing VPC's private subnets.
- EKS Pod Identity for separate relay-ingest, relay-deliver, and KEDA operator
  roles. KEDA uses operator ownership; the sink has no AWS role. IRSA is the
  pre-staging fallback only if the pinned KEDA path fails rehearsal and the
  owner accepts an ADR amendment.
- KEDA. **Both** Kafka scalers support MSK IAM, by different routes, and the two
  spellings are not interchangeable:

  | Scaler | Underlying client | MSK IAM configuration |
  |---|---|---|
  | `apache-kafka` (experimental) | `segmentio/kafka-go` | `sasl: aws_msk_iam`, `tls: enable`, `awsRegion` |
  | `kafka` | Sarama | `sasl: oauthbearer` **plus** `saslTokenProvider: aws_msk_iam` |

  An earlier draft of this roadmap said the `kafka` scaler had no MSK IAM
  support at all. That was true of the 2.12-era documentation and stopped being
  true with kedacore/keda#5692; re-checked against the KEDA 2.20 docs on
  2026-08-24. Prefer `apache-kafka` regardless, since it shares kafka-go with
  the service.

  Mixing the two spellings produces an "unexpected EOF" that reads as a
  networking fault, which is a documented source of confusion in KEDA's own
  tracker.

**Exit:** the same `ScaledObject` from M2 scales `relay-deliver` against MSK.
Success or failure reaches the destroy path by the fixed deadline. Terraform
state and explicit service inventories contain no M4 runtime resource, and a
settled cost capture records the final charge. Exact filenames and redaction
rules are in the AWS relay runbook.

---

## Later, if ever

- **Long-retry parking** stays outside M3 and M4 under
  [#88's observable trigger](https://github.com/lilabrooks/my-local-platform/issues/88):
  a request to enable retry delays whose total worst-case record work exceeds
  `config.DefaultStallBudget`. Tiered retry topics and a due-at Postgres
  scheduler remain the two recorded options. kafka-go keeps heartbeating during
  handler work, but an in-process wait stalls every partition assigned to that
  member and cannot fit relay's 30-second record and shutdown budget. The
  corrected mechanism and measurements are in
  [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md#retry-duration-is-bounded-by-how-long-one-member-may-stall).
- **SNS to SQS versus Kafka.** The Terraform already builds an SNS topic, an SQS
  queue and a DLQ (`main.tf:111-163`) -- the same fan-out in the AWS-native
  idiom. Running relay against both and writing up where each breaks down is
  more interesting than either alone, and would give ADR 0006 real evidence.
- **Karpenter** provisioning nodes underneath KEDA's pod scaling.
- **Schema and contract versioning** for event payloads.
- **Retention and tiered storage** tuning, with the cost arithmetic.

---

## Risks

| Risk | Milestone | Mitigation |
|---|---|---|
| minikube cannot reach the compose broker | M2 | Named above; budget a day, decide the listener strategy deliberately |
| kafka-go does not compose with the MSK IAM signer | M4 | Checked in M0, before any producer code exists |
| Demo-first ordering leaves semantic gaps | M2 | Known and accepted; the gaps are enumerated as M3 rather than left implicit |
| MSK left running after M4 | M4 | Budget alarm before apply; `Ephemeral=true` tag; `make aws-cost` |
| CI runtime grows with each new service | M1 | Acceptable; revisit if the smoke job exceeds ~5 minutes |

Estimates assume focused sessions and are the least reliable thing here.

## Verification

**As written on 2026-08-24, nothing here had been verified by running it** — it
was a plan, not a record. The cost figures were checked against AWS published
pricing on that date. KEDA scaler behaviour was checked against KEDA's
documentation for 2.12 through 2.21. Line citations were checked against the
files named. Everything else was a claim to be tested by building it.

M0, M1 and M2 have since been built, so that paragraph stopped describing this
document and was left standing under a header saying so until an external review
on 2026-08-30. What the milestones actually tested now lives where the evidence
belongs rather than being restated here:

| Claim | Where it was run |
|---|---|
| Replay redelivers acknowledged events | [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md#verification), and `make relay-replay-verify` on every push |
| Per-tenant ordering | [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md#verification), and `make relay-verify-ordering` on every push |
| Lag-based autoscaling beats an HPA on CPU | [ADR 0007](adr/0007-keda-lag-autoscaling.md#verification), measured 2026-08-27 |
| In-cluster Prometheus and Grafana for the demo | [ADR 0008](adr/0008-in-cluster-observability-for-the-demo.md#verification), measured 2026-08-27 |

**M3's whole-application result was verified on 2026-09-05. M4 remains
unverified.** Re-check every MSK and EKS cost figure against current pricing
before M4; both schedules change.

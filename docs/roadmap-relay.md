# Roadmap: `relay`, the first application

Date: 2026-08-24 · Last audited: 2026-08-27
Status: **M0, M1 and M2 are built and their milestones closed.** M3 and M4
remain proposed, and are alternatives rather than a sequence -- see the decision
point below. The header used to read "Proposed -- no code written".

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

The path to something demonstrable is **M0 to M1 to M2**, and the demo is the
KEDA scaling loop described in [goal-relay.md](goal-relay.md#the-demo).
Everything after M2 is an enhancement to be chosen, not a plan to be executed.

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

(That line used to point at a Resolved section in `backlog.md`. There is no
longer one: resolved entries are deleted rather than archived, because the file
records deferral rationale and the closing commit is the record of a fix.)

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

M2 is a complete, demonstrable thing. Stop here and decide whether to enhance,
extend, or leave it. M3 and M4 are alternatives, not a sequence -- M4 does not
depend on M3.

| If the goal is | Do |
|---|---|
| A service that withstands scrutiny of its semantics | M3 |
| The differentiated MSK-on-EKS story | M4 |
| Neither, for now | Nothing. M2 stands on its own |

---

## M3 -- Harden (optional)

**Free. Two to three days.**

The work that turns a convincing demo into a service whose guarantees hold up
when someone reads the code carefully.

- **Delivery attempts persisted** -- the audit trail answering "did you deliver
  my webhook?", which is the question every real webhook product exists to
  answer. `GET /v1/events/{id}/attempts`.
- **Idempotency** -- dedupe on the key accepted at ingest.
- **Tracing** -- W3C trace context in Kafka headers, so ingest, consume and
  delivery join into one trace in Tempo. Closes the
  [ADR 0005](adr/0005-argocd-gitops.md) telemetry gap for the in-cluster case.
- **Per-tenant ordering test** -- prove same-tenant events land on one partition
  and are delivered in order. An untested claim about ordering is worth nothing.
- **Head-of-line blocking** -- the per-subscriber topic described in
  [ADR 0006](adr/0006-kafka-over-sqs-for-delivery.md), if the limitation has
  actually bitten.
- **Long-retry parking** -- what the `standard` retry preset needs before M1's
  startup validation will accept it. A consumer cannot sleep 16 hours between
  attempts without being evicted from its group, so the record has to be parked
  and the offset committed: tiered retry topics, or a due-at row in Postgres
  with a scheduler. ADR 0006 records both and why neither is MVP work.

---

## M4 -- The ephemeral AWS pass (optional)

**~$1/hour. One measured session. Ends in `terraform destroy`.**

The one thing the local stack structurally cannot teach: MSK Serverless supports
**IAM authentication only** -- no SASL/SCRAM -- so this is SASL_SSL with
OAUTHBEARER tokens minted from IAM credentials, delivered to pods by IRSA or EKS
Pod Identity, with `kafka-cluster:*` IAM actions as the authorization layer
instead of Kafka ACLs.

[ADR 0004](adr/0004-real-kafka-not-emulated.md) rejected emulated MSK and left
this door open: "when the goal shifts to learning MSK's control plane." This is
that shift, narrow enough to name precisely.

### Before the first apply

1. An AWS Budgets alarm. The failure mode is not overspending during the session
   -- it is forgetting to destroy afterwards, at $700+/month.
2. Confirm the EKS version is in **standard** support. Extended support bills
   $0.60/cluster-hour instead of $0.10, applied automatically:

   ```bash
   aws eks describe-cluster-versions --query 'clusterVersions[?status==`STANDARD_SUPPORT`].clusterVersion'
   ```

3. Decide the ECR layout. `aws_ecr_repository.app` is singular
   (`infra/terraform/envs/dev/main.tf:165`) -- either a repository per service or
   one with prefixed tags.

### Scope

- `enable_msk` variable, default `false`, alongside `enable_rds` and
  `enable_eks` in `infra/terraform/envs/dev/variables.tf`.
- MSK Serverless in the existing VPC's private subnets.
- IRSA or Pod Identity for relay pods **and** the KEDA operator -- both need the
  role, which is its own lesson.
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
`terraform destroy` runs clean, `make aws-cost` confirms the meter stopped, and
an ADR records the commands and the measured result.

---

## Later, if ever

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

**M3 and M4 remain unverified in exactly the original sense**, including every
cost figure for MSK and EKS. Re-check those against current pricing before M4;
both schedules change.

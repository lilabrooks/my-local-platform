# 7. KEDA on consumer lag, not an HPA on CPU

Date: 2026-08-27
Status: Accepted

## Context

`relay-deliver` is a Kafka consumer group. When a subscriber slows down, the
backlog grows and the fix is more consumers; when it recovers, the consumers are
waste. Something has to decide when.

Kubernetes ships that something: a HorizontalPodAutoscaler, which by default
scales on CPU utilisation. Reaching for anything else needs an argument, because
an HPA on CPU is one field in a manifest and KEDA is an operator, six CRDs, and
a component that has to reach both the cluster API and the broker.

This record exists as its own issue
([#33](https://github.com/lilabrooks/my-local-platform/issues/33)) for a reason
worth repeating here: [ADR 0006](0006-kafka-over-sqs-for-delivery.md) was
accepted carrying a Verification section full of *planned* checks, and its
deciding argument -- replay -- sat unexercised through the whole of M1 until an
audit found it. Nothing forced the gap closed. So nothing below was written
before it was run.

## Decision drivers

1. **The signal has to correlate with the need.** An autoscaler is a control
   loop; if its input moves the wrong way, more tuning cannot save it.
2. **The demo has to be legible in three minutes.** Whatever drives scaling
   should be the same number a person watches on a panel.
3. **The shape should carry into M4**, where the broker is MSK and the cluster
   is EKS.
4. Cost, in components to install and understand. This is the driver that
   argues *against* the decision, and it is real.

## Options considered

### An HPA on CPU -- measured, and it fails on driver 1

This is the alternative that deserved a real test rather than a plausible
sentence, because it is what Kubernetes gives you for free.

`relay-deliver` requests `cpu: 10m`, and an HPA on `Utilization` targets a
percentage *of the request*. Two runs on the `mlp` profile, consumers pinned to
one replica so this measures the consumer rather than the scaler's reaction:

```text
PHASE A -- subscriber answering immediately, no backlog
  t        lag      cpu(cores)   % of 10m request
  t=8s     0        0.0088       88%
  t=24s    0        0.0151       151%
  t=40s    0        0.0149       149%
  t=80s    0        0.0142       142%

PHASE B -- subscriber answering in 1s, backlog of ~600
  t        lag      cpu(cores)   % of 10m request
  t=8s     595      0.0089       89%
  t=40s    574      0.0078       78%
  t=72s    545      0.0049       49%
  t=80s    536      0.0059       59%
```

**CPU is lower with a 595-event backlog than with none.**

The issue that asked for this ADR predicted the argument as "a consumer blocked
on a slow HTTP call burns no CPU while its backlog grows". The measurement says
something more specific, and worse for the HPA: the consumer burns *plenty* of
CPU -- up to 151% of its request -- precisely when there is nothing to do.

The mechanism is that **CPU tracks throughput, and lag tracks the gap between
arrival rate and throughput.** Marshalling JSON, signing an HMAC per delivery
and fetching from the broker all cost CPU per *event delivered*. A subscriber
that slows down reduces the delivery rate, so it reduces CPU -- while the
backlog it causes grows. The two are not merely uncorrelated. They are
anti-correlated, by construction.

With KEDA driving and an HPA's arithmetic applied to the same run, against a
conventional 70% target:

```text
  t       lag     pods   cpu/pod     an HPA would
  t=9s    864     10     37%         hold
  t=18s   864     12     30%         scale DOWN
  t=45s   644     12     33%         scale DOWN
  t=90s   219     12     36%         hold
  t=135s  19      12     33%         scale DOWN
  t=162s  0        5     25%         scale DOWN
```

At `t=18s` the backlog is at its peak, twelve consumers are draining it, and CPU
per pod reads 30%. An HPA would have removed consumers in the exact window they
were needed. It never exceeded 38% at any point while a backlog existed, and
reached 151% when there was none.

It is worse than a signal that does nothing, because the error compounds: CPU
per pod *falls* as pods are added, since the same work is spread wider. The more
correctly KEDA scales up, the more strongly an HPA would want to scale down.

**Rejected on measurement, not on principle.**

### An HPA on a custom metric

Publishing lag through the custom-metrics API and pointing a stock HPA at it
would work, and needs an adapter to serve that API. KEDA *is* that adapter --
`v1beta1.external.metrics.k8s.io` is registered by `keda-metrics-apiserver` --
plus a scaler library that already speaks to Kafka. Building the same thing by
hand is the same architecture with more of it written here.

### KEDA on consumer group lag -- chosen

## Decision

**KEDA `2.20.2`, one `ScaledObject` on `relay-deliver`, triggered by the
`apache-kafka` scaler on consumer group lag.**

```text
  trigger        apache-kafka
  bootstrap      host.minikube.internal:9094
  topic          mlp.relay.deliveries
  lagThreshold   10
  min / max      1 / 12
```

`apache-kafka` rather than the older `kafka` scaler because it is built on
`segmentio/kafka-go`, the client `services/relay` already uses -- one library's
behaviour to understand rather than two.

Lag satisfies driver 1 by construction: it is the size of the work not yet done.
It satisfies driver 2 because it is the same series the demo's panel plots, read
from the same broker KEDA reads.

## Consequences

**Twelve partitions is the ceiling on useful consumers, and `maxReplicaCount` is
set to it.** A consumer group assigns each partition to at most one member, so a
thirteenth pod is assigned nothing and drains nothing. The measured run plateaus
at twelve with a backlog still draining, which is the ceiling being reached
rather than the scaler hesitating.

Raising it later is not free. The partition key is the tenant id, so adding
partitions reshuffles the key-to-partition mapping and breaks per-tenant
ordering for records already on the log. `kafka-topics.sh --alter --partitions`
can only increase, never decrease. This is why
[the roadmap](../roadmap-relay.md#topic-layout) set twelve before there was any
data rather than starting at three.

**A tenant is never faster than one consumer.** Same mechanism seen from the
other end: one tenant's events all land on one partition, so twelve pods against
one busy tenant adds eleven idle pods. `local/bootstrap/relay-db.sh` seeds
sixteen `demo-NN` tenants so the demo shows the scaler working rather than the
partition assignment defeating it.

**KEDA's cost, measured:** three Deployments (`keda-operator`,
`keda-metrics-apiserver`, `keda-admission`), **six CRDs**, and **134 MiB**
resident. It also registers an aggregated APIService,
`v1beta1.external.metrics.k8s.io`, which means a broken KEDA degrades the
cluster's metrics API rather than only its own scaling.

It must reach **both** the cluster API and the broker. That is two dependencies
where an HPA on CPU has one, and it is the thing to check first when scaling
stops working.

**The scaler's credentials become the interesting part at M4.** MSK Serverless
supports IAM authentication only, so KEDA needs its own IRSA or Pod Identity
role -- the operator, not just the relay pods. The two Kafka scalers spell this
differently and the spellings are not interchangeable:

| Scaler | Client | MSK IAM configuration |
|---|---|---|
| `apache-kafka` | `segmentio/kafka-go` | `sasl: aws_msk_iam`, `tls: enable`, `awsRegion` |
| `kafka` | Sarama | `sasl: oauthbearer` **plus** `saslTokenProvider: aws_msk_iam` |

Mixing them produces an "unexpected EOF" that reads as a networking fault, which
is a documented source of confusion in KEDA's own tracker.

## Failure semantics

**KEDA unreachable or broken.** The Deployment holds its current replica count;
it does not scale to zero. Lag grows and is visible on the panel, which is the
honest failure -- nothing pretends to be handling it.

**`kubectl scale` does not hold.** The HPA KEDA manages restores
`minReplicaCount` within seconds. Stopping the consumer deliberately -- as
replay must, since Kafka refuses to move a group's offsets while it has members
-- requires the `autoscaling.keda.sh/paused-replicas` annotation.
`scripts/relay-replay.sh` does this, with a trap that removes the annotation on
exit; without it an interrupted run leaves the consumer at zero and the topic
silently not draining.

**Scaling down mid-delivery.** `terminationGracePeriodSeconds` must exceed the
maximum time one record can occupy a consumer, or KEDA's scale-down kills
in-flight deliveries. `k8s/validate` asserts this against the same retry
schedule the service validates at startup.

## Verification

Run on 2026-08-27, `mlp` profile at `--memory=6g`, Kafka and Postgres in
compose, compose apps stopped. 600 events across 16 tenants per phase.

The CPU figures above came from cAdvisor via the kubelet scrape, against the
request that an HPA would divide by:

```bash
# CPU an HPA on Utilization would see, as a fraction of the 10m request
avg(rate(container_cpu_usage_seconds_total{namespace="mlp",pod=~"relay-deliver.*"}[1m]))
kube_pod_container_resource_requests{namespace="mlp",resource="cpu"}   # 0.01 cores

# lag, and the consumer count, both from the demo's own panel queries
max(relay_consumer_group_lag_total{group="relay-deliver"})
count(relay_build_info{role="deliver"})
```

Phase A and B were run with `autoscaling.keda.sh/paused-replicas=1` so the CPU
figures describe one consumer rather than the scaler's reaction to itself. The
combined table was run with KEDA in control.

The full scale-up and drain, and the memory this costs, are recorded in
[ADR 0008's Verification section](0008-in-cluster-observability-for-the-demo.md)
rather than repeated here.

**Not verified:** everything in the M4 paragraph. The MSK IAM configuration and
the scaler spellings were checked against KEDA's documentation for 2.20 and its
issue tracker, not run. `enable_msk` does not exist yet.

## Revisit when

- **M4 begins.** The scaler gains IAM credentials and the bootstrap address
  changes; the trigger type should not.
- **A tenant's throughput becomes the constraint** rather than the group's. That
  is the partition-per-tenant ceiling, and no autoscaler can lift it -- it needs
  the per-subscriber topic [ADR 0006](0006-kafka-over-sqs-for-delivery.md)
  describes.
- **`maxReplicaCount` and the partition count disagree.** They are set equal on
  purpose and nothing enforces it; a `k8s/validate` invariant would, and would
  have been worth more than this sentence if the number ever changes.

# 8. In-cluster Prometheus and Grafana for the M2 demo

Date: 2026-08-26
Status: Accepted

## Context

[Issue #22](https://github.com/lilabrooks/my-local-platform/issues/22) put
`/metrics` on both relay roles and the sink, added scrape targets, and
provisioned a Grafana dashboard. All of it works, verified against a live stack:
lag climbed to 110 and drained to 0 across 11 partitions.

None of it is reachable from the demo. `make relay-demo`
([#32](https://github.com/lilabrooks/my-local-platform/issues/32)) runs in
minikube; Prometheus and Grafana run in docker-compose on the host. Those are
separate bridge networks on the same Docker daemon:

| Holds | Docker network | Subnet |
|---|---|---|
| Prometheus, Grafana, Kafka, Postgres | `mlp_default` | 172.18.0.0/16 |
| minikube profile `mlp` | `mlp` | 192.168.58.0/24 |

Nothing routes between them. The one crossing that exists runs the other way --
pods reach the compose broker at `host.minikube.internal:9094`, described in
[the Kubernetes runbook](../runbook-k8s.md).

Step 2 of the demo is "lag climbs on a Grafana panel". Without a decision here
it is the single step that cannot be scripted, and steps 3 and 4 depend on it
for their meaning: pods appearing with no visible reason is not a demonstration
of autoscaling, it is a demonstration of pods appearing.

This reverses a statement in an accepted record. [ADR 0005](0005-argocd-gitops.md)
says in-cluster telemetry is deliberately absent, because wiring `echo` to an
endpoint that does not resolve from inside minikube would ship a config that
silently fails. That reasoning held while the only workload was `echo` and there
was nothing to look at. It stops holding when the deliverable *is* the picture.
This amends ADR 0005 rather than superseding it: that record's subject is
ArgoCD, and only the telemetry paragraph in its Consequences changes. It is
marked as amended there, and ADR 0005 otherwise stands.

## Decision drivers

Ranked. The first one decided it.

1. **The shape must carry into M4.** On EKS you run Prometheus in the cluster
   with Kubernetes service discovery. An approach that works only on a laptop
   teaches the wrong thing and is discarded at the milestone that matters.
2. **Per-pod resolution is required.** The panel plots lag against consumer
   count, and `count(relay_build_info{role="deliver"})` is truthful only if each
   pod is its own scrape target.
3. **The blast-radius boundary in `k8s/argocd/project.yaml` should survive.**
   Its `sourceRepos` holds one entry and its `clusterResourceWhitelist` one
   kind, both deliberately, and its own comment defends the choice.
4. **One dashboard definition, not two.** #22 shipped `relay.json` for compose.
   A second copy is a panel that works in one place and silently does not in the
   other.
5. **Memory**, on a node then started with `--memory=3g`. Treated as adjustable
   rather than fixed, which turned out to matter: measuring showed 3g cannot
   run this at all, and `MINIKUBE_MEMORY` is now 6g. See Verification.

## Options considered

**Bridge the Docker networks.** `docker network connect mlp mlp-prometheus`,
then scrape NodePorts. One command, and it fails driver 2 outright: a NodePort
load-balances across endpoints, so each scrape lands on an arbitrary pod, the
consumer count reads 1 forever, and `relay_records_consumed_total{partition}`
blends several pods into one incoherent series. Carries nothing into M4.

Note that this objection is specific to reaching pods *through a Service from
outside*. It does not apply to the chosen option: a `ServiceMonitor` makes
Prometheus discover Endpoints and scrape each pod IP individually.

**In-cluster Prometheus in agent mode, remote-writing to the host Prometheus.**
Discovery happens inside the cluster where it works; data crosses over
`host.minikube.internal`, the direction already proven by Kafka. One Grafana,
one dashboard file, no new inbound path, and the shape matches Prometheus agent
to AMP on EKS. The cheapest option satisfying every driver. Rejected because it
splits where metrics are collected from where they are stored, which is one more
thing to explain inside three minutes.

**Full Prometheus in-cluster, Grafana staying on the host.** Halves the memory
and removes the dashboard-duplication problem entirely. Rejected because the
demo would point a host Grafana at a cluster Prometheus, a hybrid M4 does not
have.

**Prometheus and Grafana both in-cluster.** Chosen. Most faithful to what EKS
runs; costs the most memory and creates the dashboard-duplication problem
addressed below.

## Decision

**`kube-prometheus-stack`, installed whole, by a pinned `make monitoring-install`
target rather than by ArgoCD.**

Chart version **88.5.4**, `prometheus-operator` **v0.93.1**, checked against
`helm search repo prometheus-community/kube-prometheus-stack --versions` on
2026-08-26.

The chart rather than hand-written manifests, because it is what a real cluster
runs and the point of M2 is to demonstrate the real thing. Installed whole --
Alertmanager, node-exporter and kube-state-metrics included, none of which the
demo uses -- because a trimmed install becomes a thing to explain later and the
memory is available.

### Not through ArgoCD

This is the part worth reading twice, because everything else here is
GitOps-managed. Routing the chart through ArgoCD requires three changes to
`k8s/argocd/project.yaml`:

- adding `prometheus-community.github.io` to `sourceRepos`, which holds exactly
  one entry today;
- adding a `monitoring` namespace to `destinations`;
- widening `clusterResourceWhitelist` from `Namespace` alone to CRDs,
  ClusterRoles and ClusterRoleBindings.

That loosens a boundary whose own comment reads: "The default project allows any
repo to deploy anything anywhere; this one does not." KEDA already sets the
precedent for the alternative -- a cluster capability installed by a pinned
target with `--server-side`, while *applications* sync from git. Monitoring is
the same category, so the boundary stays as written.

What still goes through ArgoCD, from this repository: the `ServiceMonitor` for
the relay roles and the sink, and the dashboard `ConfigMap`.

### Supporting decisions

- **Per-pod scraping via `ServiceMonitor`.** Prometheus discovers Endpoints and
  scrapes each pod IP, so `count(relay_build_info{role="deliver"})` stays
  truthful as KEDA scales the group.
- **One dashboard file.**
  `local/config/grafana/provisioning/dashboards/relay.json` stays canonical and
  is embedded in the ConfigMap the chart's Grafana sidecar collects. A new
  invariant in `k8s/validate` fails when the two diverge -- this repository's
  existing habit of turning "the YAML looks right" into a failing test.
- **Grafana reached by port-forward on a free port**, not 3000. The compose
  Grafana holds 3000, and this repository has been bitten by a port collision
  once already: the sink sits on 8084 because `make argocd-ui` took 8081.
  `make relay-demo` forwards it and prints the URL.
- **`scripts/relay-replay.sh` learns a cluster mode.** Kafka refuses to move a
  group's offsets while it has members, so the script stops the consumer with
  `docker compose stop`. In-cluster that lever does not exist, and
  `kubectl scale --replicas=0` does not hold -- the KEDA HPA restores
  `minReplicaCount` within seconds, which the runbook already records. Cluster
  mode annotates the ScaledObject `paused-replicas=0`, waits for the drain,
  resets, then unpauses. `make relay-replay-verify` keeps running compose mode
  in CI.
- **`make relay-demo` is the watchable path**: no hands, eyes on Grafana. It
  asserts its own *preconditions* -- see the preflight in Consequences -- but
  not its outcomes; what the six steps demonstrate is read off the panel by a
  human. The self-checking path is a separate target shaped like the existing
  `relay-replay-verify`. The distinction is worth keeping: a demo that judged
  its own success would need to encode what "lag drained" means, and the
  argument it exists to make is visual.
- **`make monitoring-install` calls Helm directly, and Helm joins the
  requirements.** The alternative was rendering the chart to a pinned manifest
  in the repository, which would have kept the toolchain unchanged and made the
  applied YAML reviewable in a diff. Rejected because it buys review at the
  price of a regeneration step that has to be remembered on every version bump,
  and a generated 6,900-line manifest is not meaningfully reviewed anyway.

  This departs from `make keda-install` in mechanism while matching it in
  category: both are cluster capabilities installed at a pinned version outside
  ArgoCD, but KEDA applies a released manifest with kubectl and this shells out
  to Helm. The chart has no released flat manifest to apply, so the divergence
  is forced rather than chosen.

  Helm is needed by nothing else here. Every other manifest is applied by
  kubectl or synced by ArgoCD, and no CI job touches the cluster, so
  `.github/workflows/ci.yml` does not need it either.

## Consequences

**A `ServiceMonitor` without the release label is silently ignored.** The chart
renders its Prometheus with:

```yaml
serviceMonitorSelector:
  matchLabels:
    release: "monitoring"
serviceMonitorNamespaceSelector: {}
```

The namespace selector is empty, so any namespace is eligible -- but the label
selector is not. A `ServiceMonitor` in `k8s/manifests/relay/` that omits
`release: monitoring` is picked up by nothing, reports no error, and leaves the
demo panel empty. This is precisely the "config that silently fails" ADR 0005
set out to avoid, arriving by a different route, and it couples a manifest in
this repository to the Helm release name chosen in the Makefile.

**Every ingest replica publishes the same lag, so panels must aggregate with
`max`, never `sum`.** This was found by running it, on 2026-08-27.

The reasoning for putting the poller in ingest included the claim that ingest is
single-replica. It is not: `k8s/manifests/relay/deployment-ingest.yaml` runs two,
both poll the broker for the same consumer group, and both publish the same
numbers. The compose stack runs one, which is why nothing showed there.

What survives is the property that actually mattered -- the series do not move
when the consumer group rebalances. What breaks is naive aggregation:
`sum(relay_consumer_group_lag_total)` multiplies lag by the ingest replica
count, and `relay_consumer_group_lag` unaggregated draws each partition once per
replica. Both were wrong in `relay.json` until a run reported a peak of 1103 for
600 events produced; with `max` the same run reads 596. Fixed, with the reason
recorded on the panels themselves rather than only here.

**The label stays, and the guard is a runtime assertion rather than a static
test.** An earlier draft of this ADR proposed a `k8s/validate` invariant
comparing the release name in the Makefile against the label in the YAML. That
was the wrong layer. Such a test proves only that two files this repository
controls agree with each other: install with a different `--release` and it
passes while the panel is still empty. It also covers one cause of "no targets"
out of at least five -- the others being a wrong port name, a mistaken Service
selector, a `ServiceMonitor` ArgoCD has not synced yet, and a future change to
the chart's own default.

The check that covers all of them is to assert, before the demo produces a
single event, the exact query the panel plots:

```text
count(relay_build_info{role="deliver"}) >= 1
```

If that returns data the panel works, whatever the cause would have been. It
costs no new plumbing: #22 verified that Grafana's `/api/ds/query` answers for
this datasource uid, so the preflight reuses the port-forward the demo already
opens instead of adding a second one to Prometheus.

With the failure made loud, the coupling is an inconvenience rather than a
hazard, and the label can be chosen on fidelity instead of safety. It stays,
because requiring it is what a shared cluster does and driver 1 is that the
shape carries into M4. Setting
`prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues: false`
removes the coupling outright -- verified: it renders `serviceMonitorSelector:
{}`, which with the already-empty namespace selector matches every
`ServiceMonitor` in the cluster -- but it is a setting to reconsider the moment
that cluster has another tenant, so taking it here would undercut the reason the
chart was chosen.

The override would also be narrower than it appears. It clears only
`serviceMonitorSelector`; `podMonitorSelector`, `probeSelector`, `ruleSelector`
and `scrapeConfigSelector` all keep `release: "monitoring"`. The same trap
returns with the first `PrometheusRule`, which is M3's alerting work.

**Two observability stacks exist.** Compose keeps its own for non-Kubernetes
work; the cluster gets its own. They share exactly one artifact, `relay.json`,
guarded by the validate test.

**The blast-radius boundary is untouched**, at the cost of monitoring not
appearing in the ArgoCD UI beside the applications. Someone will ask why. The
answer is the one that already applies to KEDA.

**Memory grows and the runbook's table becomes wrong.** It currently says the
cluster costs ~1.8 GB on top of a ~1.6 GB stack. `make k8s-up` pins
`--memory=3g` and will need raising. The new figure must be measured, not
estimated.

> **Measured amendment, 2026-08-27.** The run below established a 3.64 GiB
> peak under load and showed the supporting stack could not run within 3 GiB.
> `MINIKUBE_MEMORY` is now 6g, and the local runbook carries the measured
> progression. The paragraph above is the predicted consequence that prompted
> the measurement, not the current configuration.

**Another pinned version to maintain**, alongside `KEDA_VERSION` and the
Kubernetes version.

**M4 inherits the shape but not the manifests.** On EKS the same
`ServiceMonitor` and dashboard apply; the install path becomes whatever that
cluster uses.

## Failure semantics

**Monitoring absent, or present but not scraping.** `make relay-demo` fails
loudly before producing a single event, and the check is the panel's own query
rather than the existence of the pieces behind it:
`count(relay_build_info{role="deliver"}) >= 1`. Checking that the Grafana
Deployment and the `ServiceMonitor` CRD exist -- what an earlier draft proposed
-- passes in every case where they exist and nothing is scraped, which is the
case that actually happens.

If every ingest scrape disappears after that preflight, **Age of the broker
measurement** displays `NO INGEST SCRAPED` in red.

**A scrape target down mid-demo.** The lag line is unaffected: lag is published
by `relay-ingest` reading the broker, and every ingest replica reports the same
value, so losing one changes nothing. A missing deliver target shows as the
consumer count dropping, which is honest.

That property is load-bearing and was nearly lost. An earlier draft said "ingest
is single-replica" -- it is not, the Deployment runs two -- and the panels
summed across them, doubling lag. See the correction under Consequences.

**The broker poll itself failing.** `relay_lag_refreshed_timestamp_seconds` goes
stale and the dashboard's freshness panel passes its threshold. The query uses
the oldest timestamp from the scraped ingest instances, so one failed poller
cannot borrow the other's fresh timestamp. It covers lag and group assignments
together. A rebalance also holds the last stable assignment and lets this age
until the broker reports a complete generation again. Relay logs that as an
informational transition. `relay_lag_refresh_errors_total` is reserved for
failed refresh attempts; the growing age records the incomplete snapshot.

**Replay interrupted in cluster mode.** This failure has no compose equivalent.
If the script dies between pausing the ScaledObject and unpausing it, the
consumer stays at zero replicas and the topic silently stops draining -- a
cluster left broken by a demo that appeared to end. Cluster mode needs a trap
removing the annotation on exit, and the demo must verify replicas recovered
before reporting step 6 as passed.

## Verification

**Run on 2026-08-27**, on the `mlp` minikube profile at `--memory=6g`, with
Kafka and Postgres in compose and the compose apps stopped. The two chart
claims were checked earlier against `helm template kube-prometheus-stack
--version 88.5.4`.

Checked:

- **The datasource uid matches.** The chart provisions its Prometheus datasource
  as `uid: prometheus`, which is what #22 pinned for compose and what
  `relay.json` names. The dashboard is portable between the two Grafanas with no
  edit. This was the most likely way this decision could have failed, and it
  does not.
- **The Grafana dashboard sidecar watches every namespace** (`NAMESPACE: ALL`,
  `LABEL: grafana_dashboard`, `LABEL_VALUE: "1"`), so a ConfigMap deployed into
  `mlp` by ArgoCD is collected by a Grafana running in `monitoring`.
- **`serviceMonitorSelector` requires the release label**, quoted above.
- **Helm 4 renders the chart.** The template above was produced by Helm
  **v4.2.4**, which is what is installed here. The chart declares `apiVersion:
  v2` and `kubeVersion: '>=1.25.0-0'`, and the cluster runs v1.35.1. Helm 3.x
  was not tested, so the requirement is stated as the version actually used
  rather than as a floor nobody has checked.

Measured:

**Per-pod discovery works, which is what driver 2 rests on.** Prometheus lists
each relay pod as its own target at its own pod IP, not one Service address:

```text
relay-ingest     http://10.244.0.24:8080/metrics    up
relay-ingest     http://10.244.0.23:8080/metrics    up
relay-deliver    http://10.244.0.22:8080/metrics    up
```

**`count(relay_build_info{role="deliver"})` tracks the Deployment** through a
full cycle. 600 events across 16 tenants, sink slowed to 1s:

```text
  t       lag    replicas   node memory
  t=0s      0     1         3.41 GiB
  t=12s   596     1         3.46 GiB    burst lands
  t=24s   582     3         3.44 GiB    KEDA reacts
  t=35s   557     7         3.52 GiB
  t=47s   507    11         3.56 GiB
  t=58s   380    12         3.58 GiB    ceiling
  t=92s   127    12         3.64 GiB    peak memory
  t=115s   19    12         3.57 GiB
  t=127s    7    10         3.47 GiB    draining, scaling down
  t=149s    0     3         3.43 GiB
  t=172s    0     1         3.43 GiB    back to idle
```

Every event was delivered; no dead letters. A peak lag of 596 against 600
produced is the arithmetic working -- the burst outruns one consumer, and the
group drains it once KEDA has added members.

**These replace an earlier set that were wrong.** The first run summed lag
across ingest replicas and reported a peak of 1103, roughly double. Memory and
replica counts were unaffected; only the lag column changed.

**The memory figure: 6g, and 3g does not work.** Peak was 3.64 GiB of the 6 GiB
cap -- 61%, with 2.3 GiB of headroom -- and one restart in the whole cluster,
in KEDA, during install.

At `--memory=3g` the same components never reached load. The supporting cast
alone sat at 88-92% with `relay-deliver` at zero replicas, `helm install` failed
on a post-install hook timing out against the API server, and the control plane
thrashed: 22 restarts in kube-system including etcd, the apiserver, the
scheduler and the controller-manager, plus 17 in ArgoCD and 12 in KEDA.

**The node cannot warn about this**, which is why the failure reads as
unrelated application crashes. minikube's kubelet reports the Docker VM's memory
as node allocatable, not the container's cgroup limit, so three numbers disagree
and nothing reconciles them:

| Source | Reported |
|---|---|
| Sum of pod memory requests, what the scheduler counts | 782 MiB |
| Node `allocatable` | 7.75 GiB |
| The Docker cgroup limit actually enforced | 3.00 GiB |

`MemoryPressure` stayed `False` throughout. Nothing was evicted in an orderly
way; the kernel killed processes inside the container and they surfaced as
`Error exit=1` rather than `OOMKilled`. ArgoCD declares no memory requests at
all, so the scheduler's 782 MiB is optimistic on top of being irrelevant.

**`make relay-demo` runs all six steps, in 190 seconds**, and refuses rather
than half-runs: no cluster, missing Deployments, no ScaledObject, the compose
consumer still running, or Prometheus not scraping all fail at step 0.

**Replay in cluster mode redelivers the window and leaves the ScaledObject
unpaused**, including when interrupted. Tested by sending SIGTERM three seconds
into a run: the `paused-replicas` annotation was gone afterwards, so the
consumer was not stranded at zero replicas with the topic silently not
draining. That was named as this record's one failure mode with no compose
equivalent, so it is checked rather than assumed.

**The `k8s/validate` invariant fires when `relay.json` is edited without
regenerating the ConfigMap.** Previously only checked by deliberately breaking
it. On 2026-08-30 it caught a real edit: correcting this dashboard's
description of the lag panel produced

```text
--- FAIL: TestDashboardConfigMapMatchesTheSourceFile
    the ConfigMap and ../../local/config/grafana/provisioning/dashboards/relay.json
    have diverged. Run: make monitoring-dashboard
    (source is 17686 bytes, ConfigMap payload is 17420)
```

`make monitoring-dashboard` regenerated it and the test passed. An invariant
that has only ever been tested by breaking it on purpose is a weaker claim than
one that has stopped a change nobody intended to make.

**M3 added broker-side group assignment evidence on 2026-09-05.** The check was
run on the existing 1-node, 6g `mlp` profile with Kafka and Postgres in compose
and relay, sink, Prometheus, Grafana, and KEDA in minikube:

```bash
make test
make lint
make k8s-validate
make up-apps
make seed
make smoke
make relay-image
make k8s-apply-local
kubectl -n mlp rollout restart deployment/relay-ingest deployment/relay-deliver
make monitoring-ready
make relay-demo
```

The compose check returned `/readyz` as ready after the consumer joined and
published `relay_group_members=1`, `relay_group_unassigned_members=0`, and
`relay_topic_partitions_unassigned=0`. The zeroes were present time series with
a current `relay_lag_refreshed_timestamp_seconds`. In minikube,
`make monitoring-ready` found every assignment series and a measurement under
30 seconds old. The one demo run produced 600 events, reached a measured lag of
593, scaled from 1 consumer to 12, drained lag to 0, and returned to 1 consumer
before the failing-subscriber and replay steps passed.

A second review found that Kafka's assignment bytes can describe an older
generation while a classic group is rebalancing. The same command sequence was
run again after relay began withholding those transitional snapshots and the
freshness query began selecting the oldest scraped ingest timestamp. Both ingest
replicas logged a withheld `PreparingRebalance` poll during the demo. After the
replay, Prometheus reported their measurement ages as 16.982 and 13.982 seconds;
the assignment gauges reported 0 idle members and 0 partitions without an owner.
A 5-second range query showed 12 unowned partitions only during the initial
no-consumer interval, then 0 from 19:36:35 UTC through the later scale changes;
idle members stayed at 0. The second 600-event run reached lag 575, scaled from
1 consumer to 12, drained at 110 seconds, returned to 1 at 140 seconds, and
completed the failing-subscriber and replay steps. These are two local runs.
They establish the checked path and do not claim a rate or reliability
distribution.

Nothing in this section is still outstanding.

### What closes this gap

**This record was accepted ahead of its own evidence, deliberately and with a
named way out.** That is the failure
[#33](https://github.com/lilabrooks/my-local-platform/issues/33) exists to
prevent, quoted from its own text: ADR 0006 "was accepted carrying a
Verification section full of planned checks", its deciding argument sat
unexercised through the whole of M1 until an audit found it
([#20](https://github.com/lilabrooks/my-local-platform/issues/20)), "and nothing
forced the gap closed".

Repeating that here would be worse than doing it the first time, because the
pattern is now documented. So the way out is named rather than assumed:
**[#40](https://github.com/lilabrooks/my-local-platform/issues/40) carries an
acceptance criterion to replace the list above with measured results and the
commands that produced them.** It cannot be closed while this section still says
nothing has been run.

The distinction from ADR 0006 was only that: not that this record was better
evidenced at the time -- it was not -- but that something outside it failed if
the evidence never arrived.

**The gap is closed.** The Verification section above was replaced with measured
results on 2026-08-27, run on the `mlp` profile at `--memory=6g`, and
[#40](https://github.com/lilabrooks/my-local-platform/issues/40) closed against
that criterion. The mechanism worked as designed: an issue outside the record
forced the evidence in, rather than the record being trusted because it read
well.

This paragraph used to end "treat every claim in the Decision and Consequences
sections as reasoning rather than as result. The two checked items above are the
exceptions." Both statements outlived the evidence arriving -- there are now
four checked items and a measured table -- and stood for three days until an
external review on 2026-08-30. Kept here rather than deleted, because a record
about the cost of stale evidence claims going unnoticed should say that its own
did.

## Open questions

None outstanding. Two were raised during the interview and both are settled
above: how `make monitoring-install` invokes the chart, and whether the Helm
release name coupling should be removed or guarded. `monitoring` is the release
name assumed throughout this record.

## Rollback

Uninstall the chart and revert the `ServiceMonitor` and ConfigMap. Keep the 6g
profile cap; 3g is a recorded failure point, not a safe rollback target.
Nothing in relay or the sink changes: #22's `/metrics` endpoints are useful
with or without this, and the compose dashboard is untouched. The code remains
cheap to reverse; what has been spent is the decision.

## Revisit when

- **M4 begins.** The install path changes and this Decision section stops
  applying, though the `ServiceMonitor` and dashboard carry over.
- **The AppProject widens for some other reason.** The argument for keeping
  monitoring outside ArgoCD is that widening costs something. If that has
  already been paid, the argument is gone.
- **A second dashboard appears.** One file guarded by one test is proportionate;
  three files are not, and generating the ConfigMap becomes worth its
  complexity.
- **A `PrometheusRule`, `PodMonitor` or `Probe` is added** -- M3's alerting work
  is the likely trigger. Each has its own selector still requiring
  `release: "monitoring"`, so the silent-drop failure returns for that kind, and
  the preflight above does not cover it. Whatever guards it should assert the
  rule is loaded rather than that the label is present.
- **Memory stops being available.** Installing the chart whole was chosen
  because it was. Alertmanager and node-exporter are the first things to cut.

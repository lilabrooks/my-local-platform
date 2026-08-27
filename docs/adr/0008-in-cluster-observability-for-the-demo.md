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
5. **Memory**, on a node started with `--memory=3g`. Adjustable, but it has a
   cost the runbook already tracks.

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

**A scrape target down mid-demo.** The lag line is unaffected: lag is published
by `relay-ingest` reading the broker, and ingest is single-replica. A missing
deliver target shows as the consumer count dropping, which is honest.

**The lag poll itself failing.** `relay_lag_refreshed_timestamp_seconds` goes
stale and the dashboard's freshness panel passes its threshold. This already
works and is what distinguishes a drained topic from a lag nobody could measure.

**Replay interrupted in cluster mode.** This failure has no compose equivalent.
If the script dies between pausing the ScaledObject and unpausing it, the
consumer stays at zero replicas and the topic silently stops draining -- a
cluster left broken by a demo that appeared to end. Cluster mode needs a trap
removing the annotation on exit, and the demo must verify replicas recovered
before reporting step 6 as passed.

## Verification

**Nothing below has been run.** Two mechanism claims were checked against the
rendered chart on 2026-08-26 (`helm template kube-prometheus-stack --version
88.5.4`); everything else is a claim to be tested by building it.

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

Still to run:

- All scrape targets `up` in the in-cluster Prometheus, with `relay-deliver`
  appearing as N separate targets at N pod IPs while KEDA scales. This is the
  claim driver 2 rests on.
- `count(relay_build_info{role="deliver"})` tracking
  `kubectl get deploy relay-deliver` through a full scale-up and scale-down.
- The `k8s/validate` invariant failing when `relay.json` is edited without
  regenerating the ConfigMap.
- `make relay-demo` refusing to start against a cluster where monitoring is
  absent, and separately against one where it is installed but the
  `ServiceMonitor` is not being selected -- the second is the case the preflight
  exists for, and the one an existence check would pass.
- Replay in cluster mode redelivering the window and leaving the ScaledObject
  unpaused, including when the script is interrupted mid-run.
- `make mem` and node pressure with the whole chart running, to set the minikube
  memory figure.

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

The distinction from ADR 0006 is only that: not that this record is better
evidenced -- it is not -- but that something outside it fails if the evidence
never arrives. Until then, treat every claim in the Decision and Consequences
sections as reasoning rather than as result. The two checked items above are the
exceptions and are marked as such.

## Open questions

None outstanding. Two were raised during the interview and both are settled
above: how `make monitoring-install` invokes the chart, and whether the Helm
release name coupling should be removed or guarded. `monitoring` is the release
name assumed throughout this record.

## Rollback

Uninstall the chart, revert the `ServiceMonitor` and ConfigMap, restore
`--memory=3g`, and the cluster is where it is today. Nothing in relay or the
sink changes: #22's `/metrics` endpoints are useful with or without this, and
the compose dashboard is untouched. Cheap to reverse -- what has been spent is
the decision, not the code.

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

# Kubernetes and GitOps runbook

Local Kubernetes runs in a **dedicated minikube profile called `mlp`**, so it
does not disturb any other cluster on the machine.

## First run

```bash
make k8s-up          # start the mlp cluster
make echo-image      # build the echo image and load it into the cluster
make argocd-install  # install ArgoCD and register the app-of-apps
make k8s-status
```

`make argocd-install` prompts before touching whatever context is current.
Set `ASSUME_YES=1` to skip that in automation.

## The GitOps loop needs a git remote

ArgoCD pulls manifests from a URL. It cannot read your working tree. Until this
repository is pushed to GitHub with these files on `main`, applications report:

```text
failed to list refs: authentication required: Repository not found.
```

That is expected. Two ways forward:

```bash
# Iterate on manifests without git, using kubectl directly:
make k8s-apply-local

# Or, once the repo is pushed, point ArgoCD at it:
make argocd-install REPO_URL=https://github.com/<you>/my-local-platform
```

### When the repository is private

A private remote cannot be cloned anonymously. Without credentials the
applications report:

```text
failed to list refs: authentication required: Repository not found.
```

One command fixes it:

```bash
make argocd-repo-creds
```

That generates a **read-only SSH deploy key**, registers it on the GitHub
repository, stores the private key in an ArgoCD Secret, and repoints the
Applications at the SSH URL.

A deploy key rather than a personal access token, deliberately: a deploy key is
scoped to this one repository, while a PAT with `repo` scope can read and write
every repository you own. Read-only rather than read-write for the same reason
— ArgoCD only ever pulls, and a writable key sitting in a cluster is a way for
a compromised workload to rewrite your git history.

The private key is written to `~/.ssh/` and into the cluster. It never enters
the repository.

A public remote needs no deploy key when every Application and `REPO_URL` use
its HTTPS URL. Change those tracked URLs when repository visibility changes;
leaving an SSH URL in one child Application still requires SSH credentials.
`make k8s-validate` checks that all child Applications, the install script, and
the Make default use the same URL.

## Project boundaries

`make argocd-install` installs 3 AppProjects. Each has one job:

| Project | Application | Authority |
|---|---|---|
| `mlp-root` | `root` | create `Application` objects in `argocd` |
| `mlp` | `echo`, `relay`, `sink`, `monitoring` | deploy into namespace `mlp` |
| `default` | none | disabled |

The root reads `k8s/apps/`, so it needs permission to register child
Applications. It has no workload or cluster-resource permission. Each child
uses `mlp`; that project permits namespaced resources in `mlp` and the
cluster-scoped Namespace named `mlp` for `CreateNamespace=true`.

Both installation scripts create `mlp-root`, move the root Application, narrow
`mlp`, then disable `default`. This order also upgrades a cluster created with
the old single-project configuration. `make k8s-validate` checks the projects,
the script order, and every tracked child Application.

The installer is fixed at ArgoCD v3.5.1. The `name: mlp` restriction on the
Namespace permission needs ArgoCD v3.3 or newer, so an environment override to
an older version would silently weaken the boundary the project claims.

[ADR 0009](adr/0009-separate-argocd-control-and-workload-projects.md) records
the boundary, its remaining limit, and the condition that would require a
Kubernetes admission policy.

## The UI

```bash
make argocd-password   # prints the initial admin password
make argocd-ui         # port-forwards to https://localhost:8081
```

Log in as `admin`. The self-signed certificate warning is expected.

## Adding a workload

The app-of-apps means you do not run `kubectl` to add an app:

1. Put manifests under `k8s/manifests/<name>/`.
2. Add an `Application` at `k8s/apps/<name>.yaml` pointing at that path.
3. Commit and push.

The root Application watches `k8s/apps/`, notices the new file, and creates the
app. Adding a directory of manifests without step 2 does nothing.

## Images

There is no registry in the local setup. `make echo-image` builds and runs
`minikube image load`, and the Deployment uses `imagePullPolicy: IfNotPresent`
so the loaded image is used rather than a pull being attempted.

After rebuilding an image, restart the workload — the tag has not changed, so
Kubernetes has no reason to notice:

```bash
make echo-image
kubectl rollout restart deployment/echo -n mlp
```

## Reaching the compose Kafka from a pod

Use `host.minikube.internal:9094`. Not `localhost:9092`, and not `kafka:19092`.

A Kafka client is told where to go after it bootstraps, rather than reusing the
address it dialled, so **whatever a listener advertises has to resolve from the
client's own network**. The broker runs three client listeners for that reason:

| Listener | Advertises | For |
|---|---|---|
| `INTERNAL` | `kafka:19092` | other compose containers |
| `HOST` | `localhost:9092` | the laptop |
| `CLUSTER` | `host.minikube.internal:9094` | pods in minikube |

Point a pod at `:9092` and it bootstraps fine, then fails on the follow-up
connection, because `localhost` inside a pod is the pod:

```text
WARN Connection to node 1 (localhost/127.0.0.1:9092) could not be established.
```

That reads as a broker fault and is not one. The broker is healthy; it just
handed out an address that means something else where the client is standing.

`host.minikube.internal` rather than the host's LAN address because minikube
provides it and a hardcoded `192.168.x.y` stops working the moment the laptop
changes network. It resolves only inside minikube, which is correct -- that
listener has exactly one audience. Verified with a pod round trip:

```bash
kubectl run kafkatest --rm -i --restart=Never --image=apache/kafka:4.3.1 \
  --image-pull-policy=IfNotPresent --command -- bash -c \
  'echo hi | /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server host.minikube.internal:9094 --topic mlp.events'
```

M4 changes this wiring again: MSK is reached over its own bootstrap endpoint
with IAM, and none of these three listeners apply.

## Autoscaling on consumer lag

KEDA scales `relay-deliver` on the lag of its consumer group. `make keda-install`
puts KEDA in the cluster; the `ScaledObject` lives with the manifests.

A measured run, 600 events across 16 tenants with the sink answering in 1s:

```text
  t=15s   lag=0     replicas=1     idle
  t=30s   lag=581   replicas=5     backlog lands, KEDA reacts
  t=45s   lag=498   replicas=10
  t=60s   lag=346   replicas=12    ceiling, draining ~10/s
  t=90s   lag=103   replicas=12
  t=120s  lag=5     replicas=8     scaling down
  t=150s  lag=0     replicas=1     back to idle
```

Three things have to be true or the run does not show what it looks like it
shows.

**The slow sink has to succeed slowly, not time out.** Set its latency below
`RELAY_DELIVERY_TIMEOUT`. A first attempt used 2000ms against a 2s timeout, so
every delivery timed out, burned all five retries plus 15s of delays -- about
25s a record -- and dead-lettered. Throughput collapsed to ~0.6/s, KEDA
oscillated, and it read as KEDA misbehaving. 205 events went to the
dead-letter queue. Nothing was wrong with the autoscaling.

**Events have to span many tenants.** The partition key is the tenant id, so one
tenant's events all land on ONE partition and exactly one consumer can ever work
on them. Twelve pods against one busy partition adds eleven idle pods and drains
nothing. `local/bootstrap/relay-db.sh` seeds sixteen `demo-NN` tenants for this.

**The compose apps have to be down**, per the section below, or half the work
happens where nobody is looking.

Note that `kubectl scale --replicas=0` does not stop a KEDA-managed Deployment:
the HPA restores `minReplicaCount` within seconds, and the pod rejoins the
consumer group. Pause it instead:

```bash
kubectl -n mlp annotate scaledobject relay-deliver \
  autoscaling.keda.sh/paused-replicas=0 --overwrite
```

Remove the annotation to hand scaling back.

## Two Grafanas, and knowing which one you are looking at

There are two complete observability stacks, and they show different data. This
is the single easiest thing to get wrong here, because both render the same
dashboard and look identical.

| | Compose | In-cluster |
|---|---|---|
| Grafana | <http://localhost:3000> | <http://localhost:3001> (`make monitoring-ui`) |
| Started by | `make up-obs` | `make monitoring-install` |
| Scrapes | the compose relay and sink | the pods in `mlp` |
| Login | anonymous viewer | anonymous admin |
| Dashboard | `/d/relay-delivery` | `/d/relay-delivery` |

**Same uid, same title, same panels, different source.** Looking at 3000 while
the cluster does the work shows a flat line and nothing wrong with it -- the
compose relay is stopped, so there is genuinely nothing to plot. The port is the
only thing that tells you which you have.

3001 rather than 3000 for exactly that reason. Two Grafanas fighting over one
port would be worse: whichever bound first would answer, and the URL would stop
meaning anything. The sink already sits on 8084 because `make argocd-ui` took
8081.

The dashboard JSON is shared. `local/config/grafana/provisioning/dashboards/relay.json`
is canonical; the compose Grafana reads the file, and
`scripts/gen-dashboard-configmap.sh` embeds it in the ConfigMap the cluster
Grafana's sidecar collects. `make monitoring-dashboard` regenerates it, and
`k8s/validate` fails if the two drift.

**Before believing an empty panel, run `make monitoring-ready`.** It asserts the
query the demo actually plots -- `count(relay_build_info{role="deliver"}) >= 1`
-- rather than that the pieces exist, because a `ServiceMonitor` missing its
`release: monitoring` label is dropped by Prometheus with no error at all.

One thing worth knowing about the lag series: **aggregate with `max`, never
`sum`.** `relay-ingest` runs two replicas, both poll the broker for the same
consumer group, and both publish the same numbers. Summing multiplies lag by the
replica count. The shipped panels do this correctly; a query typed by hand in
Explore will not.

## Do not run the compose apps and the cluster apps together

They are alternatives, not complements.

`make up-apps` and `make k8s-apply-local` each start a delivery consumer, and
both join the Kafka consumer group `relay-deliver` against the same topic. Kafka
does what it should: it splits the twelve partitions between them.

```text
GROUP           HOST            #PARTITIONS
relay-deliver   /172.18.0.2     6      <- the compose container
relay-deliver   /172.18.0.1     6      <- the pod
```

Each partition is assigned to one group member at a time, so in steady state a
record is handled by whichever side owns that partition. The delivery contract
is still at-least-once: a crash after the webhook POST and before the offset
commit can redeliver the same `webhook-id`. The two sides deliver to different
sinks, so half the traffic lands somewhere you are not looking. `make smoke`
fails with

```text
FAIL  relay  45000ms  event evt_... was not delivered to the sink
```

even though nothing is broken. Stop one side:

```bash
docker compose --env-file .env -f local/docker-compose.yml \
  stop relay-ingest relay-deliver sink
```

The same applies to the M2 demo: it runs in the cluster, so the compose apps
have to be down or the lag and pod-count picture is measuring half the work.

## Troubleshooting

**`kubectl apply` fails installing ArgoCD** with `metadata.annotations: Too
long`. Client-side apply cannot handle the ApplicationSet CRD. Use
`--server-side`; the install script already does.

**App stuck `OutOfSync` after editing a manifest.** `selfHeal` reverts manual
`kubectl` edits back to git by design. Change git, not the cluster.

**App reports that its project, destination, or resource is not permitted.**
The Application crossed one of the boundaries above. Root belongs to
`mlp-root`; every file under `k8s/apps/` belongs to `mlp` and deploys to
namespace `mlp`.

**`Deployment ... spec.selector: field is immutable`.** Something added a label
to the Deployment's selector. Selectors cannot change after creation — delete
and recreate the Deployment, and use `includeSelectors: false` in the
kustomization.

**Pod stuck `ImagePullBackOff` on `echo:dev`.** The image is not in the
cluster. Run `make echo-image`.

**Deleting the root app removed everything.** That is the finalizer doing its
job: the root app owns the children. To remove ArgoCD but keep workloads,
delete the Application with `--cascade=orphan`.

## Checking manifests without a cluster

```bash
make k8s-validate
```

Renders the kustomizations and checks selector immutability, pod and Service
labels, probes, local image policy, the delivery deadline and shutdown grace
period, KEDA's replica ceiling against Kafka's partition count, and the shared
dashboard contract. It also checks the ArgoCD projects, installation order, and
every child Application's assignment. These are repository-specific tests, not
only YAML syntax.

Always run with `-count=1` (the make target does). These tests read manifests
via `kubectl kustomize`, and Go's test cache does not track those files — plain
`go test` will replay a stale PASS after a manifest edit.

## Costs

Nothing here costs money — it is all local. The EKS path in
`infra/terraform/` is a separate decision with real hourly billing behind it;
see [costs.md](costs.md).

## Shutting down

```bash
make k8s-down     # stop the cluster, keep its state
make k8s-delete   # delete it entirely
```

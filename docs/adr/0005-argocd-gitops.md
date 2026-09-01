# 5. ArgoCD for GitOps, on minikube

Date: 2026-08-23
Status: Accepted

## Context

Deployment was the missing half of the platform. Everything else — messaging,
storage, telemetry — had a story, but nothing described how code reaches a
cluster.

ArgoCD is the common answer and worth learning on its own merits: it is what a
large share of Kubernetes shops actually run, and the pull-based model it
implements is meaningfully different from pushing with `kubectl` from CI.

## Decision

ArgoCD `v3.5.1`, installed into a **dedicated `mlp` minikube profile**, using
the app-of-apps pattern.

- `k8s/argocd/` — install script, `AppProject`, root `Application`
- `k8s/apps/` — one `Application` per workload; the root app watches this
  directory, so adding a file here registers an app with no further `kubectl`
- `k8s/manifests/` — the manifests those Applications point at

A separate minikube profile matters: there was already a 3-node `minikube`
profile on this machine, and adopting it would have meant installing ArgoCD
into someone else's cluster. `mlp` is single-node, 4 CPU, 3 GB.

> **Capacity amendment, 2026-08-27.** The dedicated profile and CPU count
> remain the decision. Its memory cap is now 6 GiB after the M2 stack failed at
> 3 GiB under control-plane pressure. ADR 0008 records the measured failure and
> the replacement cap.

The `AppProject` restricts source repos and destination namespaces. ArgoCD's
`default` project permits any repo to deploy anything anywhere, which is a
strange default to inherit for a repository intended as a reference.

## Consequences

**The GitOps loop needs a reachable git remote.** ArgoCD pulls from a URL; it
cannot read a working tree. Until this repository is pushed to
`https://github.com/lilabrooks/my-local-platform` with these files on `main`,
the applications report:

```text
Failed to load target state: ... failed to list refs:
authentication required: Repository not found.
```

That is expected, not a misconfiguration. `make k8s-apply-local` applies the
same manifests directly with `kubectl`, which is the way to iterate on them
before the repo exists.

**Images are not in a registry.** `make echo-image` builds and calls
`minikube image load`, with `imagePullPolicy: IfNotPresent`. Pointing at ECR
would work identically once images are pushed there — the Terraform already
creates the repository.

**In-cluster telemetry is deliberately absent.** The OTel collector runs in
docker-compose on the host, not in the cluster. Wiring `echo` to an endpoint
that does not resolve from inside minikube would ship a config that silently
fails, which is worse than an honest gap.

> **Amended 2026-08-26 by
> [ADR 0008](0008-in-cluster-observability-for-the-demo.md), accepted.** This
> paragraph no longer holds: Prometheus and Grafana now run in the cluster,
> installed by `make monitoring-install`. The 2026-08-27 capacity amendment
> above is the other change; the rest of ADR 0005 stands.
>
> The reasoning above was not wrong for its time. It held while the only
> workload was `echo` and there was nothing to look at, and what it was
> protecting against -- shipping a config that silently fails -- is still the
> right instinct. ADR 0008 inherits it: the chart's `serviceMonitorSelector`
> drops an unlabelled `ServiceMonitor` without an error, so the demo asserts
> the panel's own query before it starts rather than trusting that the pieces
> exist.

## Verification

Installed and checked on the `mlp` cluster (Kubernetes v1.35.1):

- All 7 ArgoCD pods reach `Running`.
- **The sync engine works.** A temporary `Application` pointed at the public
  `argoproj/argocd-example-apps` repo reached `Synced` / `Healthy` in ~18s and
  created its Deployment and Service from git. Removed afterwards. This proves
  reconciliation works in this cluster, which "the pods are running" does not.
- The `echo` manifests apply cleanly from scratch and both replicas serve
  traffic: `/`, `/healthz` and `/metrics` all respond over the ClusterIP
  Service.
- **The GitOps loop is verified end to end.** The repository is now a private
  GitHub remote with a read-only deploy key. Pushing a commit that changed
  `replicas: 2` to `3` produced a third running pod ~12s later with no
  `kubectl` involved; reverting the commit scaled it back down just as fast.

Verifying it also exposed a flaw in the placeholder scheme. `install.sh`
substitutes `__REPO_URL__` as it applies `k8s/argocd/*.yaml`, but `k8s/apps/`
is read by ArgoCD **straight from git**, where nothing substitutes anything.
The placeholder synced literally and the app failed with `application repo
__REPO_URL__ is not permitted in project 'mlp'`. The rule:

| Directory | Applied by | Placeholder |
|---|---|---|
| `k8s/argocd/` | `install.sh` | works, substituted at apply time |
| `k8s/apps/` | ArgoCD, from git | needs a real URL |

`selfHeal` was confirmed the hard way while fixing it: applying the corrected
Application with `kubectl` was reverted to the git version within seconds. The
fix had to go through git, which is the entire point.

Two install details worth recording:

- `kubectl apply` **fails** on the ArgoCD manifests. Client-side apply stores
  the manifest in a `last-applied-configuration` annotation, and the
  ApplicationSet CRD exceeds the 262144-byte limit. `--server-side` is required.
- kustomize's `commonLabels` writes labels into the Deployment's **selector**,
  which is immutable after creation, so adding one later breaks `apply`. The
  fix is `labels:` with `includeSelectors: false` and `includeTemplates: true`
  — the label reaches pods without touching the selector.

That second one is a bug that appears only on the *second* apply, so
`k8s/validate` asserts it as a Go test alongside three other manifest
invariants. The tests were checked by reintroducing the bug and confirming they
fail, rather than by assuming a passing test means anything.

One trap in those tests, recorded because it nearly hid the check: they read
YAML through `kubectl kustomize`, and **Go's test cache keys on Go sources, not
on the manifests**. Editing a manifest and re-running plain `go test` replays a
cached PASS. The first attempt at this verification did exactly that and
reported success against known-broken input. `make k8s-validate` and CI both
pass `-count=1`.

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

```
failed to list refs: authentication required: Repository not found.
```

That is expected. Two ways forward:

```bash
# Iterate on manifests without git, using kubectl directly:
make k8s-apply-local

# Or, once the repo is pushed, point ArgoCD at it:
make argocd-install REPO_URL=https://github.com/<you>/my-local-platform
```

For a private repo, ArgoCD also needs credentials — `argocd repo add` with a
token, or a `repo-creds` Secret.

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

## Troubleshooting

**`kubectl apply` fails installing ArgoCD** with `metadata.annotations: Too
long`. Client-side apply cannot handle the ApplicationSet CRD. Use
`--server-side`; the install script already does.

**App stuck `OutOfSync` after editing a manifest.** `selfHeal` reverts manual
`kubectl` edits back to git by design. Change git, not the cluster.

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

Renders the kustomization and asserts four invariants: the Deployment selector
stays free of mutable labels, pods still carry them, the Service selector
matches the pods it is meant to reach, and the container has both probes.

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

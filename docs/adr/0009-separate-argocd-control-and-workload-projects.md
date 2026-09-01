# 9. Separate ArgoCD control and workload projects

Date: 2026-08-31
Status: Accepted

## Context

The pre-publication audit found an escape path in the AppProject boundary.
The root Application and every workload Application used `mlp`. That project
allowed deployments into both `argocd` and `mlp`, allowed `Application`
objects through its namespaced wildcard, and allowed Namespace resources.

A child Application chooses its own project in `spec.project`. It could name
ArgoCD's built-in `default` project instead of `mlp`. The
[ArgoCD project guide](https://argo-cd.readthedocs.io/en/stable/user-guide/projects/#the-default-project)
documents that `default` initially accepts any repository, destination, and
resource kind. It can be emptied, though ArgoCD does not allow deleting it.

That made the comment in `k8s/argocd/project.yaml` stronger than the enforced
policy. Review of `k8s/apps/` was the real boundary. Repository visibility
leaves Git write permissions unchanged, but a public reference repository
should state and test the mechanism it claims.

## Decision drivers

1. Enforce the namespace and resource split in ArgoCD itself.
2. Preserve the app-of-apps flow and the current workload manifests.
3. Keep the local cluster free of another admission controller.
4. Make upgrades idempotent and safe to retry after interruption.

## Options considered

### Keep the single project

The repository could keep `mlp` and treat review of every child Application as
the control. This has no migration cost. It leaves `default` as a working
bypass and keeps the checked-in description false, so it was rejected.

### Split the projects and disable `default`

Give the root Application a project that can create only `Application` objects
in `argocd`. Keep workloads in a project confined to `mlp`. Empty the built-in
`default` project using ArgoCD's documented manifest. This closes the current
bypass with ArgoCD resources already installed in the cluster. Chosen.

### Enforce Application fields with cluster admission policy

A `ValidatingAdmissionPolicy`, Kyverno, or Gatekeeper rule could require the
root Application to use `mlp-root` and every child to use `mlp`. This would
also stop a child from selecting a future privileged AppProject. It adds a
cluster-scoped policy and another installation responsibility for a local,
single-owner platform. That cost is deferred until the revisit condition below
occurs.

## Decision

Three AppProjects define the path:

| Project | Used by | Destination | Resource authority |
|---|---|---|---|
| `mlp-root` | root Application | `argocd` | `argoproj.io/Application` only |
| `mlp` | child Applications | `mlp` | namespaced resources, plus Namespace `mlp` |
| `default` | nothing | none | none |

Both active projects accept one repository URL, substituted by the installation
scripts. The `mlp` Namespace permission carries a name restriction, so
`CreateNamespace=true` cannot create another namespace.

The representative path is now:

1. The root Application reads `k8s/apps/` from the configured repository under
   `mlp-root`.
2. `mlp-root` permits the resulting child `Application` objects in `argocd`.
3. Every tracked child names `mlp` and reads its workload path from the same
   repository.
4. `mlp` permits those resources only in namespace `mlp`.

`k8s/validate` discovers every tracked child Application and checks its project
and destination. It also checks all 3 AppProjects, the root Application, and
the manifest order in both installation scripts.

## Migration and failure semantics

The scripts apply resources in this order:

1. create `mlp-root`;
2. move the root Application to it;
3. narrow `mlp` to the workload namespace;
4. empty `default`.

That order keeps an existing root Application valid during an upgrade. A new
installation can briefly show child project errors before step 3 finishes;
rerunning the idempotent script completes the same sequence.

An Application with a disallowed source, destination, or resource receives an
ArgoCD comparison or permission error and its disallowed resources are not
applied. The live resources already owned by other valid Applications remain
under those Applications. Operators should inspect the Application condition
rather than treating a healthy ArgoCD control plane as proof that a rejected
sync succeeded.

## Consequences

The root can register workloads without receiving workload permissions itself.
The workload project retains full namespaced authority inside `mlp`, which is
the intended trust boundary for code merged into this repository. Cluster
administrators remain outside this boundary.

ArgoCD AppProjects do not constrain the value of `spec.project` inside an
`Application` resource created in the `argocd` namespace. Locking `default`
closes the project that currently provides broader authority. A future
privileged AppProject could reopen the path if child Applications can select
it. The repository tests catch tracked project changes, and the revisit
condition below calls for admission policy before such a project is added.

The project manifests and root Application remain bootstrap resources. ArgoCD
does not manage them from `k8s/apps/`; changing them requires rerunning
`make argocd-install` or `make argocd-repo-creds`.

## Verification

Checked on 2026-08-31:

- `make k8s-validate` passed with the new project, root, child-discovery, and
  script-order tests.
- `bash -n k8s/argocd/install.sh k8s/argocd/repo-creds.sh` and ShellCheck both
  passed.
- A server-side dry run accepted all 3 AppProjects and the root Application on
  the existing `mlp` profile, Kubernetes v1.35.1.
- Applying the migration left the root assigned to `mlp-root`. The live
  projects matched the table above, including empty source, destination, and
  resource allow-lists on `default`.
- A temporary Application assigned to `default` immediately received
  `InvalidSpecError` for both its repository and destination. A second assigned
  to `mlp` and aimed at namespace `default` received `InvalidSpecError` for its
  destination. Both test Applications were deleted afterwards.

The root remained `Healthy` but its sync status was `Unknown`: this private
repository had no repository credential in the saved cluster, so ArgoCD could
not read `main`. Child Applications were therefore absent. The resource-kind
restriction on `mlp-root` is covered by the checked-in manifest test and the
server-accepted AppProject schema; a fresh Git sync of this unpushed branch was
not tested. Repeat that part after the branch is reachable to ArgoCD.

## Rollback

Move the root Application back to `mlp` before deleting `mlp-root`, then
restore the `argocd` destination in `mlp`. Keep `default` empty because the
working deployment path has no reason to use it. The installation scripts are
safe to rerun after either the forward migration or rollback.

## Revisit when

- another AppProject receives authority beyond `mlp`;
- Applications need destinations outside namespace `mlp`;
- separate teams receive Git or ArgoCD write access;
- workload sources split across repositories.

The first condition requires field-level admission policy before the broader
project is installed.

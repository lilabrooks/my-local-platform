# Live AWS relay validation runbook

Status: #97's approved 2026-09-22 attempt failed during worker provisioning;
both workers reported an uninitialized CNI plugin. The session was aborted,
and no application capture ran. See the
[attempt and recovery record](reviews/m4-97-live-attempt-20260922.md).
The local add-on repair requires a newly qualified candidate, new staging and
separate paid-run approval. The historical #96 source must not be rerun. The
same change repairs the stop path that left a node group outside state and adds
a plan-gate review of EKS networking prerequisites. For pod capacity, the owner
chose a third worker, so the fixed shape now runs three desired workers.

Staging for #96 completed on 2026-09-20 UTC for frozen source
`474dca7e8e121c08f2951b02871cfe4c4b87e2ee`; the issue closed when
[PR #148](https://github.com/lilabrooks/my-local-platform/pull/148) merged.
Local run `20260920T153931Z` passed all four machine rehearsals and
all four visual views. Preflight `20260920T155738Z` passed all 16 checks.
The cheap dev tier was applied, both immutable images were staged, and the
hourly plan was reviewed without applying it. GO passed at 16:13:51Z; the
[sanitized staging packet](evidence/m4-staging/20260920T155738Z/publication.json)
passed `verify-stage`. Final inventory at 16:13:47Z found two ECR repositories
and no hourly runtime. #97 was awaiting separate paid-run approval at that point.
See the [staging record](reviews/m4-96-staging-20260920.md) for commands,
results, preserved hashes and the closure boundary.

The [bounded completion plan](plan-m4-96-completion.md) governed the completed handoff.
The [rehearsal record](reviews/m4-replay-capture-rehearsal.md) preserves the
original receipts and both historical failed captures.
No command on this page authorizes an AWS mutation.

This runbook implements the contract in
[ADR 0010](adr/0010-live-aws-relay-contract.md). It is the shared handoff for
issues #92 through #97 and #136. The IAM transport, guarded Terraform plan,
clocked cleanup, cost alert, local rehearsal, deployment render, and runtime
bootstrap now exist. The remaining real-account checks and live evidence
commands remain subject to their issue gates.

The live run needs three separate approvals:

1. accept the contract in #91;
2. authorize cheap-tier staging in #96;
3. authorize the hourly EKS, MSK, and RDS apply in #97.

An earlier approval does not imply a later one.

## Fixed runtime

| Component | Shape |
|---|---|
| EKS | Kubernetes 1.35 in standard support, three desired Spot `t3.medium` nodes, range 1 to 3 |
| MSK Serverless | one cluster, 12-partition delivery topic, one-partition DLQ |
| RDS | one private single-AZ `db.t4g.micro`, 20 GB gp3 |
| Relay | two ingest pods; KEDA scales deliver from 1 to 12, which three workers fit (see [pod capacity](#infrastructure-prerequisites)); one relay image digest |
| Sink | one private `ClusterIP` pod |
| Platform | KEDA, ArgoCD, Prometheus, Grafana, Tempo in EKS |
| ECR | immutable `mlp-dev/relay` and `mlp-dev/sink` repositories; git SHA tags, digest deployments |

Only the EKS API endpoint is public, limited to the operator's current address.
Use `kubectl port-forward` for the sink, ArgoCD, Grafana, and Tempo. Do not add
an ingress or load balancer during the session.

Pod Identity associations belong to `relay-capture`, `relay-bootstrap`, `relay-ingest`,
`relay-deliver`, and `keda-operator`. The sink has no AWS role. KEDA uses
`identityOwner: keda`.

### Infrastructure prerequisites

The resolved EKS module disables `bootstrap_self_managed_addons`. AWS's
[CreateCluster API](https://docs.aws.amazon.com/eks/latest/APIReference/API_CreateCluster.html#API_CreateCluster_RequestSyntax)
documents that this suppresses VPC CNI, CoreDNS and kube-proxy. This topology
must declare them explicitly alongside the Pod Identity agent.

The EKS and VPC modules are pinned exactly. `make aws-plan` runs
`scripts/check-aws-plan.py`, which reviews the saved plan's end state. With
self-managed bootstrap disabled, the gate requires all four add-ons with known
versions (an unpinned version is unknown at plan time), the VPC CNI as a
before-compute add-on, and `AmazonEKS_CNI_Policy` on every managed node group's
role. The summary's `eks_dependencies` records the result. The 2026-09-22 saved
plan fails this gate.

Placement is not ordering. The module creates add-ons without
`before_compute` after the node groups, which cannot become Ready without the
CNI. `before_compute` only removes that wait: the module starts those add-ons
with the cluster and delays compute by 30 seconds, without waiting for them.
CoreDNS and kube-proxy wait for the node groups and must be ready before
application bootstrap. Save the dated `aws eks describe-addon-versions` output
for the four pins privately with the staging evidence.

The VPC CNI uses the worker role, and aws-node runs on the host network, so the
IMDSv2 hop limit of 1 does not block it. Workload Pod Identity associations do
not supply that permission. Changing the CNI identity is a separate reviewed
configuration change that also needs a gate change; see
[AWS's CNI permissions](https://docs.aws.amazon.com/eks/latest/userguide/cni-iam-role.html).

Pod capacity is part of this review, and local rehearsal cannot show it:
minikube allows 110 pods per node. With the VPC CNI's default networking, each
`t3.medium` worker allows 17 pods, as the 2026-09-22 nodes reported. aws-node,
kube-proxy, the Pod Identity agent and node-exporter take 1 slot each per
worker, leaving 39 on 3 workers. The rendered stack uses 22 of them before any
delivery pod: CoreDNS 2, KEDA 3, monitoring 5, ArgoCD 7, and relay ingest,
sink, collector and Tempo 5. That leaves 17 for delivery pods, so KEDA's
maximum of 12 fits with 5 spare, fewer while a Job runs. Two workers fit only
about 4, which is why ADR 0010's
[worker capacity amendment](adr/0010-live-aws-relay-contract.md#worker-capacity-amendment-accepted-2026-09-22)
raised the desired count to three, the node group's maximum. At that maximum
there is little room to replace a Spot worker before it stops, so a reclaimed
worker can leave two until its replacement joins, with delivery pods beyond
about 4 Pending meanwhile. Recheck this arithmetic if the rendered stack
changes.

The capture's replica series is `kube_deployment_spec_replicas`, the desired
count, so it would not show pods left Pending. Record running replicas and
Pending pods beside desired replicas before relying on scaling evidence, and
record any worker lost during the capture. Do not weaken the capture's
acceptance to fit.

Then trace the remaining prerequisites through the existing deployment:
private network paths and image pulls, workload identity, secret/config
handoff, MSK topics and RDS initialization, KEDA, and the observability stack.
Use the shared [dependency review](application-validation.md#review-runtime-dependencies)
for new applications; only include the capabilities their contract uses.

## Stop conditions

Do not apply if any of these is false:

- `aws sts get-caller-identity` is the intended account and role;
- no account id or credential value is present in a tracked or staged file;
- Kubernetes 1.35 is in EKS standard support in `us-east-1`;
- current published inputs keep the modeled shape at or below $1.25/hour;
- the reviewed plan has no more than one EKS cluster, one MSK cluster, one RDS
  instance, one NAT gateway, three worker nodes, or 13 topic partitions;
- the plan summary's `eks_dependencies` gate passed, and the pod-capacity
  decision under [Infrastructure prerequisites](#infrastructure-prerequisites)
  is recorded;
- every hourly resource is opt-in, tagged `Project=my-local-platform` and
  `Ephemeral=true`, and appears in the destroy plan;
- the local rehearsal for deploy, demo, abort, evidence, redaction, and cleanup
  passed at the exact commit being staged;
- the persistent `mlp-live-aws-monthly` budget has its expected notifications,
  a subscriber on each, and every notification state is `OK`; its exact filter
  is the active `Project=my-local-platform` cost allocation tag and it excludes
  tax; the reviewed plan passes project and EKS child-resource tag coverage;
- the repository owner has separately authorized this hourly apply.

During provisioning or after apply, any unexpected resource, public workload
endpoint, identity failure, shape-gate failure, standard-support mismatch, or
confirmed missing runtime prerequisite ends the attempt and starts destroy.

## Clock and spend

Create a UTC run id before staging. `make aws-live-run` writes
`.evidence/m4/<run-id>/00-session.json` immediately before apply. The receipt
contains the exact commit, region, operator, start time, 2-hour-30-minute
destroy deadline, 3-hour hard deadline, $1.25/hour shape cap, and $5.00
per-run maximum.

The controller starts a conservative clock immediately before apply. It starts
destroy at 2 hours 30 minutes even if evidence is incomplete. Do not extend the
sample to obtain a successful result. If Terraform is still applying at the
deadline or when an operator stops the run, the controller sends `SIGINT`,
waits 30 seconds, sends `SIGTERM`, waits 10 seconds, and sends `SIGKILL`.
`SIGINT` lets Terraform stop its provider plugins and record partial creates; a
plugin that receives `SIGTERM` exits mid-request. Heartbeats do not reset these
grace periods. Another operator signal advances to the next signal immediately;
after `SIGKILL`, further signals do not skip the final 5-second exit check.
Cleanup requires confirmation that the whole apply process group has exited.
If that remains unconfirmed, the controller exits with `phase: cleanup_failed`,
`cleanup_blocked_reason: process_exit_unconfirmed` and `cleanup_verified: false`,
without starting destroy. `make aws-live-status` exposes that blocked reason.
An unconfirmed exit from a cleanup command also stops
further commands. The controller's error names the process group it could not
confirm. The owner must confirm all run processes have exited, as step 2 of
[manual recovery](#controller-stopped-unexpectedly) describes, before
continuing that recovery. Resources may still be billable; the controller does
not report their cleanup as complete. Run the controller on the Mac, as below:
in a Linux container without an init process, unreaped zombies keep the group
alive and block cleanup.

The project $5 monthly AWS Budget before tax is a delayed forgotten-resource alert.
The controller enforces the session clock because billing data cannot arrive
fast enough. These are separate limits. A forecast or actual budget alarm can
block later hourly runs until AWS returns it to `OK` or the monthly period
resets.

The account collector and hourly plan/apply guard share the same live budget
check. GO requires the recorded project scope and planned tag coverage. Read
the [coverage limits](costs.md#project-budget-coverage): a zero tagged subtotal
does not establish zero cost, and some fees remain unallocated. During #97,
inspect the actual node/volume tags and service-created resources. The session
clock, inventory and destroy checks remain required even when the budget is OK.

### Waiting within the paid window

The [2026-09-22 timing record](reviews/m4-97-live-attempt-20260922.md#measured-waiting)
shows 9m58s for EKS control-plane creation, with RDS, MSK and NAT provisioned
in parallel. The managed node group then spent at least 18m30s creating
without ready networking. Abort to verified cleanup took another 23m05s.
These are one run's observations, not AWS service bounds.

Keep the existing 150-minute destroy deadline and 30-minute cleanup reserve.
Before approval, budget backward for teardown, proof/exports, deployment and
provisioning. The four deployment command caps below sum to 58m30s, before
other setup, rollout and baseline waits; proof has a 20-minute cap. Record an
earlier stop point if the remaining work no longer fits. Those caps do not
predict normal durations or authorize using the cleanup reserve for proof.

Use phase changes and readiness to decide whether to keep waiting. The
controller currently observes the apply process and overall clock; it does
not diagnose Kubernetes health during apply. The checkpoints below are an
operator responsibility. No timeout increase or live repair follows from a
slow phase.

## Configuration and secrets

The AWS overlay changes only environment-specific values:

```text
KAFKA_AUTH_MODE=aws_msk_iam
KAFKA_BOOTSTRAP=<MSK IAM bootstrap brokers>
AWS_REGION=us-east-1
DATABASE_URL=<private RDS connection string>
OTEL_EXPORTER_OTLP_ENDPOINT=<in-cluster collector>
```

Topic names, the `relay-deliver` group, retry schedule, application behavior,
metrics, and traces remain the M3 contract.

The relay image contains `/relay`, `/relay-replay`, and the operator-only
`/relay-capture` command. All use the settings above for broker operations. In IAM mode,
relay loads the ambient AWS SDK credential provider once at startup and keeps
its refresh-aware credential cache. The adapter asks the pinned AWS MSK IAM
signer for a fresh 15-minute token from kafka-go's per-connection SASL `Start`
call. It does not cache or log the signed token. The mechanism name is
`OAUTHBEARER`, and the initial response is `n,,`, a control-A,
`auth=Bearer <token>`, then two control-A bytes.

IAM mode always uses TLS 1.2 or newer with system trust and per-broker server
name verification. There is no insecure-skip or plaintext IAM setting. The
runtime reports whether failure occurred while generating or validating the
token, during the Kafka SASL exchange, or during the requested Kafka operation,
without adding credentials or token bytes to its own errors.

The AWS replay path runs `/relay-replay` in a short-lived Job under the
`relay-deliver` service account. It must not run under `relay-ingest`, whose IAM
role cannot alter consumer-group offsets. #94 owns that Job rendering and the
identity trace; the binary and its shared transport are supplied by #92.

RDS manages its master password in Secrets Manager. A second secret holds the
controlled sink signing key. The staging helper must:

1. fetch both values into process memory without putting them in arguments or
   logs;
2. stream the namespace-scoped Kubernetes Secret to `kubectl apply` over
   standard input;
3. seed the RDS subscription with the same signing key;
4. keep both values out of command output, evidence, and temporary files.

Do not copy secret values into git, images, Terraform variables or outputs,
shell history, screenshots, or evidence files.

## Rendered deployment

`k8s/base/` is the shared workload definition. `k8s/manifests/` adds the local
ConfigMap, Secret, image tags, and unauthenticated Kafka scaler. `k8s/aws/`
adds the AWS service accounts, MSK IAM scaler, internal Tempo and OpenTelemetry
collector, and references to the two stable runtime objects. A workload change
therefore has one base instead of separate local and AWS copies.

The AWS root Application is deliberately untracked. #96 records the approved
commit and two ECR digests. Render the bundle during #97 only after its
separately authorized apply has also produced the MSK IAM broker endpoint:

Use `make aws-live-capture` after successful apply, as shown under Live proof.
It reads `.image_references.relay` and `.image_references.sink` from
`06-go-no-go.json`, and reads `msk_bootstrap_brokers` from dev Terraform state
under that packet's AWS profile. These are the renderer's inputs; the operator
does not transcribe them. The helper writes a run-private kubeconfig, verifies
its explicit context against the EKS API endpoint, and passes that same path
and context to bootstrap and all three installers.

The `aws-k8s-render` subcommand writes only beneath ignored
`.evidence/m4/<run-id>/rendered/` and does not contact Kubernetes or mutate AWS.
The enclosing `aws-live-capture` command deploys and runs the proof inside the
authorized live window. The renderer also requires both image values
to match `06-go-no-go.json` for this run and commit. Its outputs are:

| File | Role |
|---|---|
| `root-app.json` | pins every child Application to the approved commit, replaces the relay and sink image fixtures with ECR digests, and supplies KEDA's MSK endpoint |
| `relay-runtime.json` | creates the non-secret runtime ConfigMap consumed by both relay roles |
| `relay-replay.json` | defines an operator-run Job using `/relay-replay` and the `relay-deliver` identity |
| `monitoring.yaml` | renders kube-prometheus-stack 88.5.4 with private Services, authenticated Grafana, and the Tempo datasource |

The populated `relay-secrets` object is not rendered to disk. The #97 run
streams it from Secrets Manager as described above. Before registering the
root Application, create the `mlp` namespace, apply `relay-runtime` and the
streamed Secret, create the delivery and DLQ topics, then create the RDS schema
and seed its subscription row with that same signing key. The adapter built by
Issue #136 owns those broker and database initialization steps; it runs only
after #97's separately authorized infrastructure apply has produced the
endpoints and the live controller reports a successful apply.
Install KEDA and the AWS monitoring values before the child Applications sync,
then pass the generated root directly to the installer. The bounded capture
helper owns this ordering. It stops on the first failed command and requests
controller cleanup before returning failure. Do not run the individual
installers as an alternative paid workflow.

`aws-runtime-bootstrap` verifies that the current Kubernetes context points at
the EKS endpoint from Terraform state. It also requires a fresh live-controller
heartbeat for the run and commit, clamps itself to the earlier of its five-minute
limit and the controller's destroy deadline, then rechecks the controller before
each mutation and while the Job runs. It creates the namespace and service
accounts, applies the rendered runtime ConfigMap, retrieves or creates the
signing value, streams `relay-secrets` with server-side apply, creates a one-shot
Job from the approved relay image, waits up to four minutes, and prints only
topic names, partition counts, and the active-subscription count. The relay
image carries the checksum-pinned `us-east-1` RDS CA bundle used by
`sslmode=verify-full`. Bootstrap is a single attempt per run ID. Any failure
ends the attempt; do not retry it inside the same paid window.

The helper targets the verified explicit context. Its presence is not
permission to run them. #95 rehearses their ordering locally, #136 packages the
adapter, #96 stages the inputs with separate owner approval, and #97 is the only
issue authorized to use the paid EKS cluster after a new approval.

Deployment has its own deadline: the controller's destroy deadline minus a
20-minute proof reserve. Bootstrap, KEDA, monitoring, and ArgoCD command bounds
are 330, 660, 660, and 1,860 seconds respectively, covering their child waits.
The earlier deployment/session deadline still wins. The helper waits for
ArgoCD-created Deployments and Services before rollout and port-forward, then
allows up to 180 seconds for a fresh one-member, one-replica, zero-lag baseline.
The proof gets at most 20 minutes; no timeout extends the paid session.

Metric samples allow up to 30 seconds of age for the relay's 15-second scrape
interval, and 60 seconds for the chart-managed replica series. The cached
88.5.4 chart documents a 30-second default; rendering the AWS values leaves
kube-state-metrics on that default. This changes the observation tolerance,
not the monitoring configuration. Recheck it when changing the chart or scrape
intervals.

After replay resumes, the final drain check requires exactly one running,
ready delivery pod among pods without a deletion timestamp. Terminating pods
can finish leaving without blocking this count. Every proof metric scrape must
postdate a resume boundary obtained from Prometheus's clock; missing clock
samples or transport interruptions retry for at most 30 seconds.

The broker check compares relay-ingest's whole-second refresh stamp with that
Prometheus boundary. Its ordering guarantee depends on clock agreement: rounding
down delays acceptance, but forward skew can admit an earlier observation.
Negative broker age is pending, not a valid sample. Both ingest replicas must
refresh after the boundary. The replay receipt retains the boundary, minimum
broker refresh stamp, and minimum scrape timestamp for each proof metric.
The existing 120-second replay-drain window and session deadline still apply.

Immediately before exporting application logs, the runner checks readiness of
all current ingest, delivery, and sink pods and their containers. Recognized
container-startup or pod-disappearance errors recheck readiness and retry the
read-only export for at most 60 seconds, within the proof/session deadline.
Other command failures, oversized output, and secret-scan failures stop capture.
A successful log export remains required; this does not repeat replay or load.

The fixed load has two observation boundaries from Prometheus: one before the
600 submissions start and one after every producer finishes. Samples used to
prove cohort scale-out must follow the first boundary. Releasing the sink delay
and completing the cohort require broker and metric observations after the
second boundary, with the same broker clock limitation described above.
Completion is checked before sampling, so producers finishing during a query
cannot make an earlier zero-lag sample eligible. The original 480-second window
and fixed 600-event, 16-tenant cohort remain unchanged.

A fresh final drain without the required scale-out observations fails that
cohort; it does not extend the sample or submit extra load. Once the load phase
starts, capture accumulates accepted samples, per-iteration observations and
pending reasons, both boundaries, and observed producer acknowledgements. It
writes `12-metrics.txt` during finalization, after requesting stop on failure
and performing local cleanup. Handled deadline, producer, sampling, and
interruption failures therefore retain partial evidence too. A process kill or
storage failure cannot guarantee an export; a failed write cannot produce a
passing receipt. A failed capture receipt remains failed even with that file.

Each iteration also reads per-ingest-instance `relay_lag_partitions_missing`,
`relay_lag_refresh_errors_total`, and `relay_lag_refreshed_timestamp_seconds`
in one optional diagnostic query, even when a proof sample is pending. Missing
or unavailable diagnostics are recorded; they do not satisfy or block proof.
The response timestamps are query evaluation times, not underlying scrape
timestamps. The broker refresh timestamp is a metric value. During group
rebalance the poller returns before updating the missing-partitions gauge or
incrementing the error counter, so zero values do not rule out a frozen poller.

A full boundary-checked sample uses 17 sequential Prometheus queries; the
optional diagnostic query makes 18 per load iteration, plus a completion-clock
query until that boundary is available. Pending samples may return earlier.
The three-second sleep follows these requests; it is not a fixed sampling rate,
and request time consumes the existing deadlines. No cohort proof sample is
accepted until every source scrape postdates its boundary, including the
30-second kube-state-metrics scrape. Each boundary can therefore leave up to
one normal KSM interval without an accepted sample, or longer if observations
are unavailable. A short peak can be missed; the diagnostic record preserves
pending iterations but does not reconstruct missing proof values.

A missing or stale sample is retried within the original baseline, 480-second
load, or 120-second replay-drain window. It cannot prove scale, release the
sink delay, or satisfy the final drain. Persistent staleness fails at the
existing deadline; other capture errors still stop immediately.

All rendered workload Services are `ClusterIP`; no AWS overlay contains an
Ingress. Grafana and ArgoCD are reached through `kubectl port-forward`. Tempo
is queried through Grafana's in-cluster datasource. `make k8s-validate` renders
both environments, checks the application-specific invariants, and runs the
rendered built-in objects through pinned kubeconform schemas without using a
cluster.

## Before staging

With the local stack and telemetry running, build and load the exact clean
candidate first. Keep ArgoCD synced to that candidate; a different Git-managed
image or ConfigMap makes this rehearsal invalid.

```bash
commit=$(git rev-parse HEAD)
make m4-images M4_SOURCE_COMMIT="$commit"
minikube image load relay:dev sink:dev -p mlp
kubectl --context mlp -n mlp rollout restart \
  deployment/relay-ingest deployment/relay-deliver deployment/sink
for app in relay-ingest relay-deliver sink; do
  kubectl --context mlp -n mlp rollout status "deployment/$app" --timeout=180s
done
```

Then run the controlled Kubernetes shutdown rehearsal. Keep its receipt outside
the live packet:

```bash
local_run_id=$(date -u +%Y%m%dT%H%M%SZ)
make m4-k8s-sigterm M4_LOCAL_RUN_ID="$local_run_id"
make m4-local-demo M4_LOCAL_RUN_ID="$local_run_id"
make m4-local-capture M4_LOCAL_RUN_ID="$local_run_id"
make aws-live-rehearse
make m4-local-abort M4_LOCAL_RUN_ID="$local_run_id"
```

This deliberately terminates one local ingest pod and one local delivery pod.
It proves that an in-flight ingest is accepted, published, and delivered after
readiness falls, and that an owned delivery completes or is safely redelivered
inside the configured grace period. See
[Rehearse controlled relay termination](runbook-k8s.md#rehearse-controlled-relay-termination)
for prerequisites and the exact checks. A passing local receipt is not AWS
authorization.

Build and load the candidate's relay and sink images first, with the local
stack and telemetry running. All four receipts must name this exact clean
commit and a passing result; the capture receipt must include image provenance
and verified local control restoration. A dirty-tree development pass cannot
substitute for this gate. Complete the visual rehearsal as well.

Create a UTC run id, then run the account-independent preflight from the exact
commit whose images will be staged:

```bash
run_id=$(date -u +%Y%m%dT%H%M%SZ)
commit=$(git rev-parse HEAD)
make aws-preflight AWS_RUN_ID="$run_id" M4_LOCAL_RUN_ID="$local_run_id"
```

The command requires a clean worktree. It checks the recorded M3 closure,
disabled hourly flags, AWS render inputs, the state-backed destroy command,
required local tools, repository tests, both commit-labelled workload images,
Terraform validation, and rendered Kubernetes objects. It does not read AWS
identity or create a cluster. A pass writes the private receipt
`.evidence/m4/<run-id>/00-preflight.json` and the fixed
`capture-plan.json`. A failed run writes the preflight receipt with the failed
check and returns non-zero. Use a new run id after fixing a failure.

The capture plan puts account, price, plan, inventory, image, and final decision
work before the paid window. Prepare the terminal commands and browser layout
before the apply. The controller creates the session clock later, after every
staging receipt and the final GO decision pass. The live sequence uses the
export order and Prometheus queries recorded in `capture-plan.json`.

The #96 staging issue must capture these files under the raw evidence directory:

- `01-identity.txt`: repository and caller identity, state-backend controls,
  budget notification, remaining quotas, regional offerings, and EKS support;
- `02-prices.md`: date, official URLs, rates, quantities, arithmetic, and gate;
- `03-plan-summary.json`: resource types, addresses, counts, and shape result;
- `04-inventory-before.json`: tagged and service-native inventories;
- `05-images.json`: commit SHA, immutable tags, and ECR digests;
- `06-go-no-go.json`: one cross-check of every staged gate, the cleanup owner,
  image digests, capture order, and stop limits.

Inspect the account-scoped state bucket before initializing guardrails. Under
the separate #96 approval, confirm the intended account, then make this
read-only check using the selected profile:

```bash
make aws-whoami
env -i HOME="$HOME" PATH="$PATH" AWS_REGION=us-east-1 \
  AWS_PROFILE="${AWS_PROFILE_NAME:-aws-public-change-feed}" \
  bash -c 'aws s3api head-bucket --bucket "mlp-tfstate-$(aws sts get-caller-identity --query Account --output text)"'
```

An access error does not prove absence. Resolve it before proceeding. Only if
the bucket is confirmed absent and the #96 approval covers its creation, run
`make aws-bootstrap`. Both guardrails and dev use this bucket.

Then create the persistent cost alert under the separate #96 approval. Copy
the private variable example, replace its email address, review the saved plan,
and apply that exact plan:

```bash
cp infra/terraform/guardrails/terraform.tfvars.example \
  infra/terraform/guardrails/terraform.tfvars
${EDITOR:-vi} infra/terraform/guardrails/terraform.tfvars
make aws-guardrails-plan
make aws-guardrails-up
```

The guardrail stack uses the bootstrap bucket with its own remote-state key.
It has no destroy target, and the budget has `prevent_destroy = true`. For an
older checkout, create this replacement before a reviewed cheap-tier dev apply
removes `mlp-dev-live-runtime` from the dev state.

Then capture the account gates before changing any dev resource:

```bash
make aws-account-check \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit"
```

This target makes read-only GitHub and AWS requests. It requires the intended
repository, a caller account matching the selected SSO profile, and the
account-scoped state bucket with versioning, AES256 encryption, and all four
public-access blocks. If that bucket is absent, stop and follow the documented
`make aws-bootstrap` path under the separate #96 staging approval.

The same receipt requires the `mlp-live-aws-monthly` budget, its 3 notification
definitions, a subscriber on each notification, and an `OK` state for each. It also
requires enough remaining capacity for one EKS cluster, one MSK Serverless
cluster, one RDS instance, six Standard Spot vCPUs, and one Elastic IP. The Spot
check reserves six vCPUs for the 3-node maximum and subtracts both running
instances and unfulfilled requests. It checks Kubernetes 1.35 standard support,
published MSK service presence,
the two fixed RDS offerings, and `t3.medium` offerings in `us-east-1a` and
`us-east-1b`. The MSK check combines the account-visible Kafka region with the
linked AWS Serverless region table; the API does not expose a separate
Serverless availability operation.

The private `01-identity.txt` receipt has mode `0600`. It contains the account
id, caller ARN, and budget subscriber address, so never stage it. Publication
uses the sanitizer described below. The final decision rejects an account
receipt older than 24 hours or one whose backend controls changed.

Create the private pricing worksheet, recheck every linked AWS page, and edit
the worksheet with the current `us-east-1` rates. Set `checked_at` to the UTC
review time, name `checked_by`, and set `confirmed` to `true` only after every
rate has been checked. The generated rate fields are blank:

```bash
make aws-price-template \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit"

${EDITOR:-vi} ".evidence/m4/$run_id/price-input.json"

make aws-prices \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit"
```

The validator refuses a review older than 24 hours, a changed source list, an
unknown or missing rate, a different run or commit, and a recomputed total over
$1.25/hour. It writes itemized `02-prices.md` plus a private JSON sidecar used by
the release gate. This step reads local files only.

Apply the cheap dev tier before capturing inventory or staging images. Keep
hourly flags disabled in ignored variable files and pass them explicitly here:

```bash
make aws-plan AWS_RUN_ID="$run_id" AWS_APPROVED_COMMIT="$commit" \
  AWS_TF_ARGS='-var enable_eks=false -var enable_msk=false -var enable_rds=false'
jq '{shape, planned_hourly_resource_counts, created_hourly_resource_counts, gate}' \
  ".evidence/m4/$run_id/03-plan-summary.json"
```

Review all actions, including deletions and migrations. Stop unless every
hourly flag is false, every hourly resource count is zero, and the gate passes.
Only after that review and the separate cheap-apply approval:

```bash
cp -p ".evidence/m4/$run_id/03-plan-summary.json" \
  ".evidence/m4/$run_id/03-plan-summary-cheap.json"
make aws-up AWS_RUN_ID="$run_id" AWS_APPROVED_COMMIT="$commit"
```

This applies the saved plan. Keep the cheap summary privately because the next
plan replaces `03-plan-summary.json`.

The inventory combines `resourcegroupstaggingapi get-resources` with explicit
EKS, MSK, RDS, EC2, EBS, Elastic IP, ELB, ECR, NAT gateway, and CloudWatch
log-group queries. The tagging API alone is insufficient. Capture the
pre-hourly-apply inventory after cheap apply, with the account selected for staging:

```bash
make aws-inventory-empty \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit" \
  AWS_INVENTORY_FILE=".evidence/m4/$run_id/04-inventory-before.json"
```

The helper writes only to the ignored run directory with mode `0600`. It checks
project tags, names, Kubernetes cluster tags, and service-native results. ECR
repositories are allowed before the run; GO requires exactly the two staged
repositories. They are forbidden in `21-inventory-after.json`. Any matching
EKS, MSK, RDS, EC2, EBS, Elastic IP, load balancer, NAT gateway, or log group
makes the command fail after preserving the receipt. Reuse the same command
after destroy with `AWS_INVENTORY_FILE` set to
`21-inventory-after.json`.

Build and stage the EKS images after the cheap tier has created both ECR
repositories:

```bash
make aws-stage-images \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit"
```

This builds `linux/amd64` images for the `t3.medium` node group. The helper
requires a passing preflight receipt for the same run and commit, then checks
that the worktree still has that clean HEAD. It reads the repository URLs from
Terraform state and requires immutable tags plus scan-on-push. Each missing
40-character commit tag is pushed once. An existing tag is pulled by digest
and its platform and OCI revision label are checked. The resulting
`05-images.json` records both digest references with mode `0600`.

Use `aws-inspect-images` for a read-only repeat:

```bash
make aws-inspect-images \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit"
```

Now replace the cheap plan with the hourly plan, without applying it:

```bash
operator_ip=$(curl -fsS https://checkip.amazonaws.com)
make aws-plan AWS_RUN_ID="$run_id" AWS_APPROVED_COMMIT="$commit" \
  AWS_TF_ARGS="-var enable_eks=true -var enable_msk=true -var enable_rds=true -var eks_operator_cidr=$operator_ip/32"
jq '{shape, planned_hourly_resource_counts, created_hourly_resource_counts, gate}' \
  ".evidence/m4/$run_id/03-plan-summary.json"
```

Require all three flags true, the fixed resource counts, the intended IPv4
`/32`, and a passing gate. Every re-plan removes the previous plan, summary,
and GO packet. Replanning or rewriting a receipt requires a new GO and another
review; do not apply or alter the state after this plan is approved.

Review the full saved plan for the infrastructure prerequisites as well:

```bash
(
  umask 077
  terraform -chdir=infra/terraform/envs/dev show -json \
    .terraform/mlp-reviewed.tfplan > ".evidence/m4/$run_id/plan-private.json"
)
```

Keep this file private. `make terraform-check` asserts the four pinned EKS
add-ons in mocked configuration, and the plan gate checks their placement,
known versions and the CNI policy in this saved plan (`eks_dependencies` in the
summary). Neither proves usability, which still needs live evidence, and
neither checks the remaining prerequisites or pod capacity. Record that review,
the gate result and the pod-capacity arithmetic for the planned three workers
against the saved plan hash in the staging record before GO.

After identity, budget, quota, availability, plan, inventory, and image
receipts all pass, write the final staging decision:

```bash
make aws-go-no-go \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit" \
  M4_OPERATOR="$USER"
```

The target makes no AWS request. It checks that every input names this run,
commit, and region, requires a clean exact HEAD, recomputes the price comparison,
checks both digest-pinned `linux/amd64` images, and records SHA-256 hashes of its
inputs. The controller makes `make aws-up` read this packet again, compare its
complete input manifest and original observation ages, and verify the fresh controller receipt immediately
before apply. An hourly `make aws-up` outside the controller is refused. Any
missing, stale, or mixed receipt leaves the paid apply blocked.

Before creating `00-session.json`, the controller repeats GO validation with
a 15-minute freshness reserve, covering the guarded apply's 15-minute allowance.
Original observation ages remain authoritative at apply.

Issue #96 may publish independently after its staging criteria pass. Supply a separate
checkout so tracked evidence cannot dirty the candidate awaiting #97:

```bash
python3 scripts/m4-publication-extra.py stage --run-id "$run_id" \
  --redactions-file ".evidence/m4/$run_id/redactions.json" \
  --publication-root /absolute/path/to/evidence-checkout
```

Use `verify-stage` with the same arguments to read it back. The packet under
`docs/evidence/m4-staging/<run-id>/` records the dated GO inputs; it does not
claim they remain fresh or authorize a later apply.

Historical publication checks the plan hash recorded in GO against the
run-specific, hash-bound plan summary. It never reads the shared executable
plan, which may have been removed or replaced by a later run. Execution still
requires that binary plan and a matching hash. Preserve GO and all eight input
files: replanning under the same run ID removes its GO and summary, and those
missing historical inputs cannot be reconstructed from a later plan.

Trace one image and configuration value through its producer, generated AWS
Application, ArgoCD load, Deployment, running pod, and evidence output. Trace
each Pod Identity association through service account, role, policy resource,
consumer, and a denied action outside its authority.

## Live proof

The paid run repeats M3's outcome on the fixed AWS topology:

Refresh the selected SSO login with `make aws-login` immediately before the
run. Check credential recovery for cleanup; login alone does not prove a
three-hour credential lifetime. The controller rechecks the current IPv4
against the planned `/32` before spending the run ID. A changed address needs
a new plan, GO, and review.

Start the controller in a dedicated terminal after the separate hourly-run
approval:

```bash
make aws-live-run \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit" \
  M4_OPERATOR="$USER"
```

Starting this target authorizes both the reviewed apply and automatic cleanup.
It uses `caffeinate -i` on macOS, creates `00-session.json` immediately before
apply, and remains in the foreground. Keep the Mac powered, open, and online.
Use another terminal for the capture commands below.

### Observe infrastructure while apply runs

Check phase transitions and, while waiting, about every two minutes. This is
an operator cadence, not a new service timeout. Use read-only queries against
the account, region and cluster bound to the approved plan, and retain dated
observations privately in the run directory. Avoid repeating full inventories
when one service's status answers the question.

| Earliest point | Observation | Decision |
|---|---|---|
| Control plane is creating | `aws eks describe-cluster`; controller status and remaining time | Wait within the reviewed phase budget while AWS is provisioning; inspect any service failure immediately. |
| API is available and the first node registers | Node conditions; `kube-system` Pods/DaemonSets/Deployments; `aws eks list-addons` and `describe-addon` | Correlate CNI startup with the node's reason. A CNI absent from both the plan and cluster is a confirmed missing prerequisite and requires abort. CoreDNS and kube-proxy are expected to be absent until the node group is active. If aws-node exists but is not Ready or keeps restarting, read its container log and the add-on's `describe-addon` health; an API-server or EC2 permission error that persists is a failed phase. |
| Node group is still creating | `aws eks describe-nodegroup` health plus the Kubernetes observations above | `CREATING` with no AWS health issues does not establish node readiness; on 2026-09-22 three such responses preceded the diagnosis. Inspect stagnant or failed conditions; a transient unready node alone is not a failure. A worker that terminates and is replaced (as at 17:57Z that day) is not a readiness result: record the instance and its state reason from `aws ec2 describe-instances`, then keep judging readiness from node conditions. |
| Apply succeeds, before capture | Both expected workers ready, four planned add-ons active, system networking/DNS workloads ready | Continue only if the remaining deployment and proof fit before destroy. Otherwise request cleanup. |

For Kubernetes observations, generate a run-private kubeconfig with
`aws eks update-kubeconfig --dry-run`, using an explicit profile, region and
alias. Verify its server against `describe-cluster` and pass that file and
context explicitly to every `kubectl` call, with a bounded request timeout.
Read nodes and system workload status; do not deploy a diagnostic workload or
mutate the cluster. The capture helper creates its own verified kubeconfig
after successful apply.

A confirmed failure requests cleanup immediately, even while Terraform is
applying:

```bash
make aws-live-stop AWS_RUN_ID="$run_id" AWS_STOP_REASON=infrastructure_failed
```

The reason defaults to `evidence_complete`, so a failed attempt names its own;
the capture helper records `capture_failed` itself. During apply, the
controller interrupts Terraform with `SIGINT` first and waits for the apply
process group to exit before destroy. If the bounded stop cannot confirm exit,
use the manual recovery path after confirming termination; do not start a
second Terraform process. A create that was in flight can still be
missing from state. Before trusting destroy, list each `Creating...` line in
the controller's output that has no matching `Creation complete`, and reconcile
those resources with the recovery procedure below. Keep the observed failure
and timestamp in the run record. Analysis and repair follow verified cleanup.

### Capture the application proof

After the controller reports a successful apply, execute the single bounded
deployment and machine-capture path in that second terminal:

```bash
make aws-live-capture AWS_RUN_ID="$run_id" AWS_APPROVED_COMMIT="$commit" \
  AWS_KUBE_CONTEXT="mlp-aws-$run_id"
```

It owns port-forwards, the fixed cohorts, capture Jobs, and output validation.
Every command rechecks the controller. Failure requests cleanup and skips all
remaining proof steps. Do not retry bootstrap or capture within the same run.
On success, take the screenshots below and request stop promptly.

A failure after session creation spends the run ID and triggers full dev
destroy, including staged ECR images. The pre-session refusal is earlier and
does not spend it. Any later attempt needs fresh evidence and separate owner
authority for the staging and paid work it requires.

The guarded apply must reach its final controller check within 15 minutes of
that timestamp. This allows for backend initialization and the last plan,
budget, and support checks without moving either cleanup deadline. If it takes
longer, the run is spent and cleanup starts without applying the plan.

Run `make aws-live-status AWS_RUN_ID="$run_id"` to inspect the deadline. Once
the required evidence is complete, request early cleanup:

```bash
make aws-live-stop AWS_RUN_ID="$run_id"
```

1. ArgoCD reports every application synced and healthy.
2. Post one event and repeat it with the same idempotency key. Both responses
   name the same event and MSK contains one delivery record.
3. Read the successful and exhausted subscriber attempt histories from RDS.
4. Follow one complete trace through ingest, Kafka produce, consume, and every
   webhook attempt.
5. Send the 600-event, 16-tenant load. Record lag rising, deliver scaling from
   one toward twelve, lag reaching zero, and replicas returning to one.
6. Capture exhausted and poison records from the DLQ with their source
   coordinates.
7. Replay selected records and match their resulting deliveries.

Replay uses the same consumer group as `relay-deliver`. Kafka refuses the
offset reset while that group has an active member, and KEDA's minimum of one
means `kubectl scale` cannot hold the Deployment at zero. The capture helper
pauses the ScaledObject, waits for delivery pods to leave, creates a one-shot
replay Job under `relay-deliver`, then removes the pause. Its export retains
the offset-reset result, selected event, before/after successful delivery
counts, and final lag and replica sample. A failure ends the paid proof and
requests destroy; it does not authorize a manual replay retry.

Save the machine-readable results as `10-event.json`, `11-attempts.json`,
`12-metrics.txt`, `13-trace.json`, `14-keda.txt`, `15-dlq.json`, and
`16-replay.json`. Take the required screenshots in this order:

1. `argocd-apps.png`, with every child Application synced and healthy;
2. `terminal-demo.png`, after the machine checks pass;
3. `grafana-lag.png`, with lag and replicas visible across the whole run;
4. `tempo-trace.png`, with the complete selected trace visible.

Optional AWS-console orientation shots use `aws-console-eks.png`,
`aws-console-msk.png`, `aws-console-rds.png`, and `aws-console-cost.png`.
Skip them when they would delay destroy. Command output remains the source
evidence for AWS state.

A failed step is valid evidence. It does not authorize more paid debugging.
Record the failure, start destroy, and return to local rehearsal.

## Local integrated rehearsal

Run the functional and visual path against minikube before opening the paid
window:

```bash
local_run_id=$(date -u +%Y%m%dT%H%M%SZ)
make m4-local-demo M4_LOCAL_RUN_ID="$local_run_id"
```

This command makes no AWS call. It requires the `mlp` context, the Compose app
containers stopped, and the minikube relay and sink ready. It first proves the
Grafana queries are populated, then exercises delivery, broker lag, KEDA scale
out and scale in, a fresh DLQ outcome, and replay. Before either command runs,
it checks every ready relay and sink pod's OCI revision label against the
source commit. The private receipt and complete terminal transcript are written
under `.evidence/m4-local/<run-id>/` with mode 0600.

The runner restores sink controls, removes any KEDA pause, and stops its own
port-forwards on success or failure. It deliberately records visual capture as
`pending-human-capture`: a machine cannot honestly claim that the 4 required
screenshots are legible. While minikube is still warm, inspect and capture
ArgoCD, the successful terminal output, and Grafana in that order. Then stop
minikube before starting the Compose app consumers, run `make up-apps`,
`make up-obs`, and `make smoke-traces`, and capture its selected Tempo trace as
the fourth view. Keeping the 2 relay-deliver groups out of Kafka at the same
time prevents either environment from consuming the other's evidence event.
This is practice evidence only. The clean-commit run with ECR images and AWS
endpoints remains part of #97.

## Abort path

The first live failure jumps to `destroy_first` in `capture-plan.json`. Skip
the remaining demo exports and screenshots. Run the state-backed destroy, then
the complete after-inventory and provisional cost capture. Evidence
sanitization and failure analysis happen after those commands finish.

An abort does not wait for an event, trace, replay, dashboard, or optional AWS
console view. Keep any raw partial files from the failed run. The publication
allowlist will remain incomplete, which correctly prevents that attempt from
being presented as a complete proof.

Rehearse the controller transition locally before staging:

```bash
make aws-live-rehearse
local_run_id=$(date -u +%Y%m%dT%H%M%SZ)
make m4-local-abort M4_LOCAL_RUN_ID="$local_run_id"
```

These commands make no AWS call. The first runs the Go controller tests and
the existing abort-protocol tests. The second checks the real `make aws-down`
dry-run for identity, initialized state, destroy, and state-backup ordering. A
local worker then creates mode-0600 temporary credential files and simulated
hourly-resource state, waits until deployment has started, and receives
SIGTERM. The worker must skip every unfinished live export and screenshot,
remove the simulated resources, observe an empty explicit inventory, record
immediate cost capture, and remove the credential directory. Its private
write-once receipt is
`.evidence/m4-local/<run-id>/abort-rehearsal.json`.

This rehearsal proves the controller's state transitions, fixed deadlines,
bounded retries, and interruption cleanup order. It cannot prove that the AWS
provider destroys a partial resource or that service APIs return an empty live
inventory; those remain measured outcomes for #97.

## Destroy is part of the run

The controller sends success, failure, operator stop, `SIGINT`, `SIGTERM`, and
the 150-minute deadline through the same sequence:

1. stop evidence collection at the destroy deadline;
2. run the state-backed dev-stack destroy;
3. delete service-created M4 CloudWatch log groups;
4. confirm Terraform has no dev resources;
5. repeat every tagged and service-native inventory from the before snapshot;
6. capture provisional month-to-date and Cost Explorer output;
7. preserve the bootstrap state bucket and persistent cost alert.

Write the destroy transcript and exit status to `20-destroy.txt`, the complete
after inventory to `21-inventory-after.json`, and the provisional bill to
`22-cost-immediate.txt`.

The controller retries destroy and the empty inventory twice after their first
failure. It marks cleanup overdue if this sequence crosses the 3-hour hard
deadline, then records the final result in the private
`controller-state.json`. The controller never kills an active destroy at the
hard deadline.

Killing Terraform can leave the S3 state lock in place. A destroy that reports
`Error acquiring the state lock` cannot succeed on retry until that lock is
released. Use only the lock ID printed by Terraform, and force-unlock only
after confirming no apply or destroy process from this run is still active.

Before destroy, the controller compares the current AWS account with
`01-identity.txt`. Transient identity lookup failures get three attempts with
30 seconds between attempts. A confirmed account mismatch stops without
destroying; three failed lookups stop with the manual recovery command printed
to both the terminal and `20-destroy.txt`. Its direct AWS CLI calls use a
10-second connection timeout, a 30-second read timeout, one CLI attempt, and a
45-second process limit. The controller owns the visible identity retry.

Cleanup requires an empty dev Terraform state and no matching M4 EKS
cluster, MSK cluster, RDS instance, NAT gateway, Elastic IP, load balancer,
worker instance or volume, dev ECR repository, or M4 log group remains. An
empty tagging response on its own does not pass. The inventory covers ELBv2
load balancers matched by name or Project tag, not Classic ELBs or every
possible untagged resource. This is a check of the fixed topology, not an
account-wide absence guarantee; an unexpected resource ends the proof and
requires explicit recovery evidence.

`cleanup_verified: true` records those two empty checks. Cost Explorer,
destroy, log cleanup, and transcript failures keep their own exit fields. If
one fails after cleanup is verified, the result is
`cleanup_complete_with_errors`. Retry the failed evidence command; do not
start a resource hunt solely because immediate cost capture failed.

### Controller stopped unexpectedly

`make aws-live-status AWS_RUN_ID="$run_id"` reports
`controller_running: false` when the active controller PID is gone or its
heartbeat is more than 5 seconds old. A spent run cannot be restarted because
`00-session.json` already exists. Keep its files and recover in this order:

First preserve the failed snapshot, before refreshing any raw identity receipt:

```bash
make aws-evidence-publish AWS_RUN_ID="$run_id" AWS_EVIDENCE_PHASE=failed \
  AWS_REDACTIONS_FILE=".evidence/m4/$run_id/redactions.json"
```

If already published, use `aws-evidence-verify` with those arguments. Never edit
the controller result to claim a pass or overwrite its original exports. The
snapshot binds the original account and profile for recovery.
Older failed packets without those snapshot bindings fail verification. Do not
backfill them from refreshed raw inputs; preserve them and request owner review.

1. Set `profile=$(jq -er '.aws_profile' ".evidence/m4/$run_id/failed-publication/06-go-no-go.json")`.
   If credentials expired, run `make aws-login AWS_PROFILE_NAME="$profile"`.
   A failed login or identity
   check leaves cleanup unverified. Then run
   `make aws-whoami AWS_PROFILE_NAME="$profile"` and compare the account with
   `.evidence/m4/$run_id/failed-publication/01-identity.txt`. Stop if they differ.
2. Confirm that the controller and every Terraform process from this attempt
   are gone:

   ```bash
   pgid=replace-with-the-process-group-from-the-error
   pgrep -l -g "$pgid"                # must print nothing
   pgrep -fl 'm4-live-run|terraform'  # must list nothing from this attempt
   ```

   Repeat the first check for each process group the controller's error
   named, and skip it when the error named none. If a member remains, wait for
   it to exit. If one survives `SIGKILL`, do not force-unlock or start
   Terraform; restart the Mac. Before restarting, copy the run's private
   evidence, and any execution checkout under `/private/tmp`, to durable
   storage, because macOS may clear that directory. Resources keep charging
   until recovery removes them.

   Check `terraform -chdir=infra/terraform/envs/dev workspace show`
   in a shell without `TF_WORKSPACE` or `TF_DATA_DIR` overrides; require
   `default`. An unexpected workspace requires owner review before recovery.
   Run `make aws-init AWS_PROFILE_NAME="$profile" AWS_REAL_REGION=us-east-1`,
   then `make aws-down AWS_PROFILE_NAME="$profile" AWS_REAL_REGION=us-east-1`.
   If destroy reports
   `Error acquiring the state lock`, first confirm that no Terraform apply or
   destroy process from this run is active. Copy the lock ID from the error,
   then release that exact lock and retry destroy:

   ```bash
   lock_id=replace-with-the-lock-id-from-terraform
   region=us-east-1
   env -i \
     HOME="$HOME" \
     PATH="$PATH" \
     TMPDIR="${TMPDIR:-/tmp}" \
     AWS_PROFILE="$profile" \
     AWS_REGION="$region" \
     AWS_DEFAULT_REGION="$region" \
     TF_VAR_region="$region" \
     TF_WORKSPACE=default \
     terraform -chdir=infra/terraform/envs/dev force-unlock "$lock_id"
   make aws-down AWS_PROFILE_NAME="$profile" AWS_REAL_REGION="$region"
   ```

   Never force-unlock a state that an active Terraform process still owns.
   Any resource whose create was in flight when apply stopped can exist in AWS
   without a Terraform state entry. On 2026-09-22 the stop command's `SIGTERM`
   killed the provider during the node-group create; the controller now starts
   with `SIGINT`, which makes this less likely without ruling it out. Start from
   the controller output's unfinished `Creating...` lines and the post-destroy
   inventory, not from tags alone. The hourly types are the EKS cluster and its
   node groups, the RDS instance, the MSK cluster and the NAT gateway; apply
   the same identity checks to each, and delete EKS node groups before their
   cluster.

   For a node group, compare `aws eks list-nodegroups` with
   `terraform state list` before retrying a cluster deletion blocked by
   attached node groups. Preserve the group's ARN, project tag and account
   match against the failed run's receipts. Under the run's teardown authority,
   delete only that confirmed run-owned group with `aws eks delete-nodegroup`
   and wait for `aws eks wait nodegroup-deleted` before retrying `aws-down`.
   Do not import or delete an unidentified resource, and do not restart the
   spent live session. Preserve the original controller result and publish
   recovery separately after both empty checks pass.

   A delete request returning `DELETING` does not release the parent yet.
   Observe the child until the service confirms deletion, then retry the
   parent. A waiter timeout calls for another status observation and continued
   cleanup within the existing authority; it does not prove the child is gone.
   The controller's finite retries cannot repair an untracked child dependency.
   AWS documents the required order in
   [Delete a cluster](https://docs.aws.amazon.com/eks/latest/userguide/delete-cluster.html).

3. Delete only CloudWatch log groups beginning with `/aws/eks/mlp-` or
   `/aws/msk/mlp-`:

   ```bash
   for prefix in /aws/eks/mlp- /aws/msk/mlp-; do
     aws logs describe-log-groups \
       --profile "$profile" \
       --region us-east-1 \
       --log-group-name-prefix "$prefix" \
       --query 'logGroups[].logGroupName' \
       --output json |
       jq -r '.[]' |
       while IFS= read -r group; do
         [ -z "$group" ] ||
           aws logs delete-log-group \
             --profile "$profile" \
             --region us-east-1 \
             --log-group-name "$group"
       done
   done
   ```

4. Once the controller and Terraform processes are gone, collect a new read-only
   recovery observation and publish it:

   ```bash
   observation_id=$(date -u +%Y%m%dT%H%M%SZ)
   python3 scripts/m4-publication-extra.py collect-recovery \
     --run-id "$run_id" --observation-id "$observation_id" \
     --redactions-file ".evidence/m4/$run_id/redactions.json"
   python3 scripts/m4-publication-extra.py publish-recovery \
     --run-id "$run_id" --observation-id "$observation_id" \
     --redactions-file ".evidence/m4/$run_id/redactions.json"
   ```

Use `verify-recovery` with the same arguments to read it back. The collector
checks fresh STS identity, the initialized dev S3 backend and default workspace,
empty Terraform state, and the complete scoped inventory including ECR. It
rejects backend credential/endpoint overrides. Failed or absent checks yield
`cleanup_unverified`; a later attempt gets a new observation ID. These commands
perform no destroy, login, initialization, or lock repair. The destructive
recovery steps above remain subject to owner authority and process checks.

The tagging API can keep returning deleted resources after both empty checks
pass. Resolve each remaining project-tagged entry through its own service's
describe call, such as `aws ec2 describe-instances`, `describe-volumes`,
`describe-network-interfaces`, `describe-nat-gateways`,
`describe-security-groups`, `describe-security-group-rules` or
`describe-subnets` for EC2 entries. Save the results privately in the run
directory, as `97-tagging-residual-check.json` did. A `NotFound` error or a
`terminated` or `deleted` state resolves an entry; a failed query leaves it
unresolved. Retained bootstrap and budget resources are expected.

The original demonstration remains failed after recovery. Both the failed
snapshot and the linked recovery packet are retained; #97 stays open.

Failed and recovery publication write beneath `docs/evidence/` in the
executing checkout, so that checkout becomes dirty. They run after the failed
attempt and do not offer staging's separate-publication-root option. Preserve
the packets for review and a separately authorized commit; use another clean
checkout of the approved candidate for a later attempt. Do not delete recovery
history to satisfy the clean-tree gate. Keep the private run directory with
its public packets for verification.

### Settled cost after a passing run

At least 48 hours after cleanup, run
`make aws-cost-final AWS_RUN_ID="$run_id"`. It writes validated JSON to
`23-cost-final.txt` using the session's UTC dates and intended account, grouped
daily by service. It follows all pages and refuses any estimated result or
missing date. Keep waiting if AWS still reports estimated data.

The owner accepted these shared-account totals on 2026-09-11. They include
other activity in the same account and do not establish exact M4 attribution
or prove the $5 per-run maximum. `make aws-cost` remains a month-to-date view.

### Settled cost after a failed attempt

`make aws-cost-final` requires a completed controller with verified cleanup. A
failed attempt keeps its original `cleanup_failed` receipt, even after a
separate recovery observation verifies cleanup, so the target refuses it. Do
not edit that receipt. Instead, at least 48 hours after the verified recovery
observation, run the same read-only query by hand and keep a dated copy with
that observation:

```bash
(
  umask 077
  observation_dir=".evidence/m4/$run_id/recovery/$observation_id"
  account=$(jq -r .aws.account_id ".evidence/m4/$run_id/failed-publication/01-identity.txt")
  aws ce get-cost-and-usage \
    --profile "$profile" \
    --region us-east-1 \
    --time-period Start="$session_start_date",End="$day_after_recovery" \
    --granularity DAILY \
    --metrics UnblendedCost \
    --group-by Type=DIMENSION,Key=SERVICE \
    --filter "{\"Dimensions\":{\"Key\":\"LINKED_ACCOUNT\",\"Values\":[\"$account\"]}}" \
    --output json > "$observation_dir/cost-settled-$(date -u +%Y%m%dT%H%M%SZ).json"
)
```

Use the session start date and the day after the recovery observation, in UTC.
If the response has a `NextPageToken`, repeat the query with
`--next-page-token` and keep every page. Every `ResultsByTime` entry must report
`"Estimated": false`; otherwise keep the file and repeat later with a new one.
Elapsed time alone does not establish settlement. These are account-wide daily
service totals, not exact session cost. For the 2026-09-22 attempt, the earliest
collection is 2026-09-24T18:31:18Z, with `Start=2026-09-22,End=2026-09-23`.
Recheck the shared monthly budget before another paid GO.

## Sanitizing evidence

Raw files stay in ignored `.evidence/m4/<run-id>/`. Copy only sanitized files
to `docs/evidence/m4/<run-id>/`.

Replace account ids, account-bearing ARNs, ECR registry hosts, RDS and MSK
endpoints, usernames, email addresses, public IP addresses, and secret values
with stable bracketed tokens. Preserve timestamps, region, resource counts,
non-account names, commit SHAs, image digests, event ids, trace ids, partitions,
offsets, metrics, and exit status.

Before staging the sanitized directory, scan it for the known account id,
endpoints, and secret values. Any match blocks the commit.

Do this after destroy. Bootstrap has already recorded private fingerprints of
the credentials and their supported encodings, bound to the session receipt.
Keep `secret-scan.json` until final publication. Do not recover or copy deleted
credentials. Create a mode-0600 JSON file for the non-credential redactions:

```json
{
  "schema_version": 1,
  "values": {
    "ACCOUNT_ID": "replace-with-the-known-account-id",
    "OPERATOR": "replace-with-the-session-username"
  }
}
```

The publisher rejects plaintext credential fields, missing or incomplete scan
coverage, changed session bindings, and known raw or encoded credentials.
For a failed attempt, use `AWS_EVIDENCE_PHASE=failed`: this publishes only typed
terminal cleanup statuses, never a demonstration pass. It works with a valid
`not_started` or partial scan receipt while withholding raw diagnostics.

Record a human visual review in ignored `visual-review.json`. It must name the
run id, reviewer, UTC review time, `"result": "passed"`, and every required
screenshot under `files`:

```json
{
  "schema_version": 1,
  "run_id": "20260905T193000Z",
  "reviewed_at": "2026-09-05T22:15:00Z",
  "reviewer": "replace-with-the-session-username",
  "result": "passed",
  "files": [
    "argocd-apps.png",
    "terminal-demo.png",
    "grafana-lag.png",
    "tempo-trace.png"
  ]
}
```

The publisher checks PNG structure and a minimum 640 by 360 size. It cannot
inspect rendered text in pixels, so the reviewer must check each image for
account ids, endpoints, usernames, email addresses, public IPs, and secret
values.

Publish the first packet after cleanup:

```bash
make aws-evidence-publish \
  AWS_RUN_ID="$run_id" \
  AWS_REDACTIONS_FILE=".evidence/m4/$run_id/redactions.json" \
  AWS_EVIDENCE_PHASE=provisional
```

The command copies only the named allowlist, replaces sensitive text with
stable bracketed tokens, validates every JSON file, checks the screenshot
review, and records SHA-256 hashes in `publication.json`. Any missing file,
unknown published file, leak, malformed screenshot, or later edit fails
verification. Full publication also requires a clean passing AWS capture,
unchanged export hashes, and a successful, non-overdue controller with every
cleanup and evidence check passing. Desired replicas alone cannot prove scale:
the capture requires fresh broker group membership above one, then one member
with zero lag and one desired replica at the end.

After `aws-cost-final` succeeds, repeat the publication command with
`AWS_EVIDENCE_PHASE=final`. This verifies
the provisional packet before adding the settled cost. Use
`make aws-evidence-verify` with the same variables to recheck either phase.

## Issue handoff

| Issue | Required exit |
|---|---|
| #91 | owner accepts ADR 0010 and this runbook |
| #92 | IAM/TLS transport passes local tests |
| #93 | disabled and enabled Terraform plans pass their resource-shape checks |
| #94 | rendered workloads preserve this topology, identity, and evidence path (completed; issue closed) |
| #95 | the full runbook, including abort and cleanup, passes locally |
| #136 | the shared topic, schema, secret, and in-cluster bootstrap path passes offline checks |
| #96 | cheap staging, images, budget alarm, current prices, and exact plan are separately approved and captured (completed; issue closed) |
| #97 | paid proof ends in destroy, empty inventories, and a settled final cost |

Changing identity provider, public exposure, resource or partition counts,
duration, spend limits, or cleanup proof requires a contract amendment and a
new owner decision before staging continues.

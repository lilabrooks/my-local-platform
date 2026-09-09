# Live AWS relay validation runbook

Status: Contract accepted on 2026-09-05. The deployment render, local
rehearsal, staging gates, live-run controller, persistent cost alert, capture
order, and evidence sanitizer are implemented. The real-account receipts and
live AWS validation remain open. No command on this page authorizes an AWS
mutation.

This runbook implements the contract in
[ADR 0010](adr/0010-live-aws-relay-contract.md). It is the shared handoff for
issues #92 through #97. The IAM transport, guarded Terraform plan, clocked
cleanup, cost alert, local rehearsal, and deployment render now exist. The
remaining real-account checks and live evidence commands remain subject to
their issue gates.

The live run needs three separate approvals:

1. accept the contract in #91;
2. authorize cheap-tier staging in #96;
3. authorize the hourly EKS, MSK, and RDS apply in #97.

An earlier approval does not imply a later one.

## Fixed runtime

| Component | Shape |
|---|---|
| EKS | Kubernetes 1.35 in standard support, two desired Spot `t3.medium` nodes, range 1 to 3 |
| MSK Serverless | one cluster, 12-partition delivery topic, one-partition DLQ |
| RDS | one private single-AZ `db.t4g.micro`, 20 GB gp3 |
| Relay | two ingest pods; deliver scales from 1 to 12; one relay image digest |
| Sink | one private `ClusterIP` pod |
| Platform | KEDA, ArgoCD, Prometheus, Grafana, Tempo in EKS |
| ECR | immutable `mlp-dev/relay` and `mlp-dev/sink` repositories; git SHA tags, digest deployments |

Only the EKS API endpoint is public, limited to the operator's current address.
Use `kubectl port-forward` for the sink, ArgoCD, Grafana, and Tempo. Do not add
an ingress or load balancer during the session.

Pod Identity associations belong to `relay-ingest`, `relay-deliver`, and
`keda-operator`. The sink has no AWS role. KEDA uses `identityOwner: keda`.

## Stop conditions

Do not apply if any of these is false:

- `aws sts get-caller-identity` is the intended account and role;
- no account id or credential value is present in a tracked or staged file;
- Kubernetes 1.35 is in EKS standard support in `us-east-1`;
- current published inputs keep the modeled shape at or below $1.25/hour;
- the reviewed plan has no more than one EKS cluster, one MSK cluster, one RDS
  instance, one NAT gateway, three worker nodes, or 13 topic partitions;
- every hourly resource is opt-in, tagged `Project=my-local-platform` and
  `Ephemeral=true`, and appears in the destroy plan;
- the local rehearsal for deploy, demo, abort, evidence, redaction, and cleanup
  passed at the exact commit being staged;
- the persistent `mlp-live-aws-monthly` budget has its expected notifications,
  a subscriber on each, and every notification state is `OK`;
- the repository owner has separately authorized this hourly apply.

After apply, any unexpected resource, public workload endpoint, identity
failure, shape-gate failure, or standard-support mismatch stops the demo and
starts destroy.

## Clock and spend

Create a UTC run id before staging. `make aws-live-run` writes
`.evidence/m4/<run-id>/00-session.json` immediately before apply. The receipt
contains the exact commit, region, operator, start time, 2-hour-30-minute
destroy deadline, 3-hour hard deadline, $1.25/hour shape cap, and $5.00
per-run maximum.

The controller starts a conservative clock immediately before apply. It starts
destroy at 2 hours 30 minutes even if evidence is incomplete. Do not extend the
sample to obtain a successful result. If Terraform is still applying at the
deadline, the controller sends `SIGINT`, waits 30 seconds, sends `SIGTERM`,
waits 10 seconds, and sends `SIGKILL`. Cleanup starts after a final 5-second
wait. Another operator signal advances the sequence immediately. The executing
repository owner owns the controller and cleanup.

The account-wide $5 monthly AWS Budget is a delayed forgotten-resource alert.
The controller enforces the session clock because billing data cannot arrive
fast enough. These are separate limits. A forecast or actual budget alarm can
block later hourly runs until AWS returns it to `OK` or the monthly period
resets.

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

The relay image contains `/relay` and the operator-only `/relay-replay`
command. Both use the settings above for every broker operation. In IAM mode,
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

1. disable shell tracing and set `umask 077`;
2. create a private temporary directory;
3. fetch both values without putting them in arguments or logs;
4. stream the namespace-scoped Kubernetes Secret to `kubectl apply` over
   standard input;
5. seed the RDS subscription with the same signing key;
6. remove the temporary directory on success, error, or interruption.

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

```bash
make aws-k8s-render \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit" \
  AWS_RELAY_IMAGE="$relay_image_by_digest" \
  AWS_SINK_IMAGE="$sink_image_by_digest" \
  AWS_MSK_BOOTSTRAP="$msk_iam_brokers"
```

The command writes only beneath ignored `.evidence/m4/<run-id>/rendered/`. It
does not contact Kubernetes or mutate AWS. It also requires both image values
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
and seed its subscription row with that same signing key. The helper built by
Issue #96 owns those broker and database initialization steps; it runs only
after #97's separately authorized infrastructure apply has produced the
endpoints.
Install KEDA and the AWS monitoring values before the child Applications sync,
then pass the generated root directly to the installer:

```bash
kubectl apply -f k8s/manifests/namespace.yaml
kubectl apply -f ".evidence/m4/$run_id/rendered/relay-runtime.json"
# Stream relay-secrets here; do not write it to disk.
# Run the #96 staging helper to create both topics, the RDS schema, and the
# subscription before any workload starts.
make keda-install
make monitoring-install-aws
make argocd-install-aws \
  AWS_ROOT_APPLICATION=".evidence/m4/$run_id/rendered/root-app.json"
```

Those commands target the current Kubernetes context. Their presence is not
permission to run them. #95 rehearses their ordering locally, #96 stages the
inputs with separate owner approval, and #97 is the only issue authorized to
use the paid EKS cluster after a new approval.

All rendered workload Services are `ClusterIP`; no AWS overlay contains an
Ingress. Grafana and ArgoCD are reached through `kubectl port-forward`. Tempo
is queried through Grafana's in-cluster datasource. `make k8s-validate` renders
both environments, checks the application-specific invariants, and runs the
rendered built-in objects through pinned kubeconform schemas without using a
cluster.

## Before staging

First run the controlled Kubernetes shutdown rehearsal from the candidate
commit. Keep its receipt outside the live packet:

```bash
local_run_id=$(date -u +%Y%m%dT%H%M%SZ)
make m4-k8s-sigterm M4_LOCAL_RUN_ID="$local_run_id"
```

This deliberately terminates one local ingest pod and one local delivery pod.
It proves that an in-flight ingest is accepted, published, and delivered after
readiness falls, and that an owned delivery completes or is safely redelivered
inside the configured grace period. See
[Rehearse controlled relay termination](runbook-k8s.md#rehearse-controlled-relay-termination)
for prerequisites and the exact checks. A passing local receipt is not AWS
authorization.

Create a UTC run id, then run the account-independent preflight from the exact
commit whose images will be staged:

```bash
run_id=$(date -u +%Y%m%dT%H%M%SZ)
commit=$(git rev-parse HEAD)
make aws-preflight AWS_RUN_ID="$run_id"
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

Create the persistent cost alert first under the separate #96 approval. Copy
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

The inventory combines `resourcegroupstaggingapi get-resources` with explicit
EKS, MSK, RDS, EC2, EBS, Elastic IP, ELB, ECR, NAT gateway, and CloudWatch
log-group queries. The tagging API alone is insufficient. Capture the
pre-apply inventory with the account selected for staging:

```bash
make aws-inventory-empty \
  AWS_RUN_ID="$run_id" \
  AWS_APPROVED_COMMIT="$commit" \
  AWS_INVENTORY_FILE=".evidence/m4/$run_id/04-inventory-before.json"
```

The helper writes only to the ignored run directory with mode `0600`. It checks
project tags, names, Kubernetes cluster tags, and service-native results. ECR
repositories are allowed because they belong to the cheap tier. Any matching
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
plan and summary hashes, and verify the fresh controller receipt immediately
before apply. An hourly `make aws-up` outside the controller is refused. Any
missing, stale, or mixed receipt leaves the paid apply blocked.

Trace one image and configuration value through its producer, generated AWS
Application, ArgoCD load, Deployment, running pod, and evidence output. Trace
each Pod Identity association through service account, role, policy resource,
consumer, and a denied action outside its authority.

## Live proof

The paid run repeats M3's outcome on the fixed AWS topology:

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
means `kubectl scale` cannot hold the Deployment at zero. Pause the
ScaledObject, wait for every delivery pod to leave the group, create the
generated Job, and always remove the pause annotation afterward:

```bash
(
set -euo pipefail

restore_relay() {
  kubectl -n mlp annotate scaledobject relay-deliver \
    autoscaling.keda.sh/paused-replicas-
}
trap restore_relay EXIT INT TERM

kubectl -n mlp annotate scaledobject relay-deliver \
  autoscaling.keda.sh/paused-replicas=0 --overwrite
kubectl -n mlp wait --for=delete pod \
  -l app.kubernetes.io/name=relay-deliver --timeout=120s

replay_job=$(kubectl -n mlp create \
  -f ".evidence/m4/$run_id/rendered/relay-replay.json" -o name)
kubectl -n mlp wait --for=condition=complete "$replay_job" --timeout=90s
kubectl -n mlp logs "$replay_job" \
  | tee ".evidence/m4/$run_id/16-replay.txt"

restore_relay
trap - EXIT INT TERM
)
```

Use `kubectl create`, not `kubectl apply`: the Job deliberately uses
`generateName` so every replay has a distinct evidence source. Preserve its
stdout before deleting the Job; it is the raw offset-reset input that #97
correlates with event and delivery results in `16-replay.json`. If pausing,
waiting, replay, or evidence capture fails, the trap still restores normal
delivery; keep the failure as evidence rather than extending the paid run.

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

Cleanup is complete only when the dev Terraform state is empty and no M4 EKS
cluster, MSK cluster, RDS instance, NAT gateway, Elastic IP, load balancer,
worker instance or volume, dev ECR repository, or M4 log group remains. An
empty tagging response on its own does not pass.

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

1. Run `make aws-whoami` and compare the account with
   `.evidence/m4/$run_id/01-identity.txt`. Stop if they differ.
2. Run `make aws-init`, then `make aws-down`. If destroy reports
   `Error acquiring the state lock`, first confirm that no Terraform apply or
   destroy process from this run is active. Copy the lock ID from the error,
   then release that exact lock and retry destroy:

   ```bash
   lock_id=replace-with-the-lock-id-from-terraform
   profile=${AWS_PROFILE_NAME:-aws-public-change-feed}
   region=${AWS_REAL_REGION:-us-east-1}
   env -i \
     HOME="$HOME" \
     PATH="$PATH" \
     TMPDIR="${TMPDIR:-/tmp}" \
     AWS_PROFILE="$profile" \
     AWS_REGION="$region" \
     AWS_DEFAULT_REGION="$region" \
     TF_VAR_region="$region" \
     terraform -chdir=infra/terraform/envs/dev force-unlock "$lock_id"
   make aws-down AWS_PROFILE_NAME="$profile" AWS_REAL_REGION="$region"
   ```

   Never force-unlock a state that an active Terraform process still owns.
3. Delete only CloudWatch log groups beginning with `/aws/eks/mlp-` or
   `/aws/msk/mlp-`:

   ```bash
   profile=${AWS_PROFILE_NAME:-aws-public-change-feed}
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

4. Run `make aws-state-empty`.
5. Run the after-destroy `make aws-inventory-empty` command above with
   `AWS_INVENTORY_FILE=".evidence/m4/$run_id/21-inventory-after.json"`.
6. Run `make aws-cost >".evidence/m4/$run_id/22-cost-immediate.txt"`.

The run remains failed even after manual cleanup. Keep the recovery output with
its spent run id.

No earlier than 48 hours after destroy, capture the settled attributed cost in
`23-cost-final.txt`. Wait longer if AWS still marks the data incomplete. #97
closes only after the final cost and empty inventories are present.

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

Do this after destroy. Create a mode-0600 JSON file under the raw run directory
with the exact values the scanner must remove. Keep the real file ignored:

```json
{
  "schema_version": 1,
  "values": {
    "ACCOUNT_ID": "replace-with-the-known-account-id",
    "DATABASE_PASSWORD": "replace-with-the-fetched-database-password",
    "OPERATOR": "replace-with-the-session-username",
    "SIGNING_SECRET": "replace-with-the-fetched-signing-secret"
  }
}
```

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
verification.

At least 48 hours after destroy, write `23-cost-final.txt` under the raw run
directory and repeat the command with `AWS_EVIDENCE_PHASE=final`. This verifies
the provisional packet before adding the settled cost. Use
`make aws-evidence-verify` with the same variables to recheck either phase.

## Issue handoff

| Issue | Required exit |
|---|---|
| #91 | owner accepts ADR 0010 and this runbook |
| #92 | IAM/TLS transport passes local tests |
| #93 | disabled and enabled Terraform plans pass their resource-shape checks |
| #94 | rendered workloads preserve this topology, identity, and evidence path (implemented; merge closes the issue) |
| #95 | the full runbook, including abort and cleanup, passes locally |
| #96 | cheap staging, images, budget alarm, current prices, and exact plan are separately approved and captured |
| #97 | paid proof ends in destroy, empty inventories, and a settled final cost |

Changing identity provider, public exposure, resource or partition counts,
duration, spend limits, or cleanup proof requires a contract amendment and a
new owner decision before staging continues.

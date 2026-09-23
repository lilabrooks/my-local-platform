# Costs

Everything under `local/` is free. This page is about `infra/terraform/`.

Prices are `us-east-1` list rates as of September 2026 and exclude data transfer.
Treat them as the right order of magnitude, not a quote. The authority on what
you are actually spending is `make aws-cost`.

## Remote state comes first

The dev stack stores state in the account-scoped S3 bucket created by the
bootstrap stack. Create that backend once before the first dev plan:

```bash
make aws-bootstrap
make aws-init
```

Terraform 1.10 or newer uses the S3 backend's native lockfile here.

The same bucket stores the persistent cost-alert state under a separate key.
Create or update that stack before staging hourly resources:

```bash
cp infra/terraform/guardrails/terraform.tfvars.example \
  infra/terraform/guardrails/terraform.tfvars
${EDITOR:-vi} infra/terraform/guardrails/terraform.tfvars
make aws-guardrails-plan
make aws-guardrails-up
```

These commands create the project-scoped `mlp-live-aws-monthly` budget before
tax, filtered to `Project=my-local-platform`. The
stack has no destroy target, and the budget has `prevent_destroy = true`.
Planning removes any older saved guardrail plan first. Applying consumes the
reviewed plan whether the apply succeeds or fails, so a later attempt must
create a new plan.

Activate the existing resource tag for billing before relying on this budget:

```bash
aws ce update-cost-allocation-tags-status \
  --cost-allocation-tags-status TagKey=Project,Status=Active
aws ce list-cost-allocation-tags --tag-keys Project
```

Use the intended real AWS profile. The gate requires `Type=UserDefined` and
`Status=Active`; an activation request alone is insufficient. AWS can take up
to 24 hours to activate a tag. The owner must authorize this billing change.

For M4 staging, `make aws-account-check` confirms this bucket exists and still
has versioning, AES256 encryption, and all four public-access blocks before any
paid resource can enter the final decision packet. If the check cannot confirm
the bucket, inspect the caller's access first; use `make aws-bootstrap` only
when the bucket is actually absent and #96 staging has been authorized.

If this checkout previously ran `make aws-init` with DynamoDB locking, update
its saved backend configuration once:

```bash
make aws-init AWS_INIT_ARGS=-reconfigure
```

The bucket, key and region stay the same, so this operation updates local
backend metadata without moving state.

An older bootstrap state may still track the unused `mlp-tfstate-lock`
DynamoDB table. The next reviewed bootstrap apply will propose deleting it.

An older checkout may instead have ignored local dev state. Inspect it before
moving anything. If it contains resources that must be preserved, migrate it
interactively with the same backend configuration:

```bash
make aws-init AWS_INIT_ARGS=-migrate-state
```

Do not use `-reconfigure` as a shortcut when local state contains resources;
that selects the remote backend without copying the existing state.

## The cheap tier — created by default

| Resource | Billing | Idle cost |
|---|---|---|
| S3 bucket | $0.023/GB-month | ~$0 |
| SNS topic | $0.50 per million publishes, first million free | ~$0 |
| SQS queue + DLQ | $0.40 per million requests, first million free | ~$0 |
| SES identity (when `ses_sender_email` is set) | $0.10 per thousand emails | ~$0 |
| Two ECR repositories | $0.10/GB-month | ~$0 |

Leaving these standing costs approximately nothing. The bucket has a 30-day
expiry rule and ECR keeps only the last 10 images, so neither grows unbounded.

The persistent guardrail stack contains the $5 monthly budget. AWS does not
charge for the budget or its email notifications.

## The expensive tier — off by default

| Flag | Creates | Approximate monthly cost |
|---|---|---|
| `enable_rds` | `db.t4g.micro`, 20 GB gp3, single-AZ | **~$15** |
| `enable_eks` | Control plane + 3× `t3.medium` Spot + NAT gateway | **~$120** |
| `enable_msk` | MSK Serverless + 13 topic partitions | **~$0.77/hour** |

The EKS figure is a Spot ballpark: it adds the third worker at the Spot rate the
earlier ~$115 estimate implied, and that rate has not been rechecked. At the
on-demand upper bound the runtime gate uses, the control plane, three workers,
the NAT gateway and its public IPv4 address cost $0.2748/hour, about $200.60
per 730-hour month, before NAT data processing and other usage charges.

The fixed relay-validation shape in
[ADR 0010](adr/0010-live-aws-relay-contract.md) models at approximately
**$1.06/hour** before small usage charges, using rates rechecked on 2026-09-05
and the third worker added on 2026-09-22. MSK contributes about $0.77/hour of
that total. The runbook rejects a
shape above $1.25/hour, starts destroy at 2 hours 30 minutes, marks cleanup
overdue at 3 hours, and requires separate approval for a $5 maximum. It
continues an active destroy after that mark.

Breaking down `enable_eks`, because it is the one that hurts:

- EKS control plane: $0.10/hour = **~$73/month**, charged whether or not a
  single pod is running.
- NAT gateway: ~$0.045/hour = **~$32/month**, plus $0.045/GB processed.
- 3× `t3.medium` Spot instances. The runtime gate deliberately models their
  on-demand upper bound, about **$0.125/hour** together, rather than predicting
  a changing Spot discount.

### The extended-support trap

A Kubernetes version that falls out of standard support moves to **extended
support at $0.60 per cluster-hour instead of $0.10** — $438/month rather than
$73, a 6× jump applied automatically with no approval step. Those figures are
from [AWS's own EKS pricing page](https://aws.amazon.com/eks/pricing/).

This is not hypothetical. The first draft of `expensive.tf` pinned `1.31`,
which is already in extended support and would have quietly billed at the
higher rate. It is now pinned to `1.35`, in standard support until 2027-03-27
per [AWS's EKS release calendar](https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html).

Check before changing the version:

```bash
aws eks describe-cluster-versions \
  --query 'clusterVersions[?versionStatus==`STANDARD_SUPPORT`].clusterVersion' \
  --profile aws-public-change-feed
```

Local Kubernetes has no such cliff — minikube is free. Use EKS when the goal is
EKS specifically.

All three hourly flags default to `false`.

## Guarded plans and applies

`make aws-plan` writes the binary plan to
`infra/terraform/envs/dev/.terraform/mlp-reviewed.tfplan` and a redaction-safe
summary to `.evidence/m4/<run-id>/03-plan-summary.json`. Both `AWS_RUN_ID` and
`AWS_APPROVED_COMMIT` are required. The guard requires a clean worktree at that
exact commit for tracked and ordinary untracked files. Terraform's ignored
variable files can still feed the plan, so the summary records a SHA-256 over
the command arguments and every automatically or explicitly loaded variable
file. The summary also records its capture time, selected resource addresses
and actions, counts, flag values, the fixed topology, and cost arithmetic. The
full plan can contain account data and stays ignored.

Issue #93 replaces the former `mlp-dev` ECR repository with `mlp-dev/relay` and
`mlp-dev/sink`. An old state that still owns `mlp-dev` will plan its deletion,
including any images it contains. The safe summary includes ECR change actions
so the operator sees that migration before apply.

`make aws-live-run` applies that exact saved plan for the same run and commit.
It creates the session clock, keeps the controller in the foreground, and
starts cleanup after an operator stop, failure, signal, or the 150-minute
destroy deadline. If apply is still running then, the controller interrupts it
with `SIGINT` and allows 30 seconds for a graceful exit, then sends `SIGTERM`,
waits 10 seconds, and sends `SIGKILL`. Cleanup waits for the whole apply
process group to exit. If the final 5-second check cannot confirm termination,
the controller records `cleanup_failed` with
`cleanup_blocked_reason: process_exit_unconfirmed` and starts no destroy.
The owner must confirm termination and continue manual recovery; charges may
continue until the resources are removed. Heartbeats never restart the grace
periods. An hourly `make aws-up` requires
the controller's fresh, run-bound heartbeat and permit.

After apply succeeds, `make aws-runtime-bootstrap` requires the same live
controller heartbeat. It verifies the current EKS context, reuses or creates
the short-lived signing value, streams the Kubernetes Secret, and runs the
idempotent MSK topic and RDS schema setup inside the VPC. Its deadline cannot
outlive the paid session, and it rechecks the controller before mutations and
throughout the Job. A stale input, stopped controller, wrong cluster context,
or failed bootstrap ends the attempt. Run deployment through
`make aws-live-capture`: it requests controller cleanup on failure and skips
the remaining commands. Do not retry inside the paid window. The controller's
deadline remains the backstop, not the normal response to a failed bootstrap.

The apply does not create a fresh plan. It requires a fresh
`06-go-no-go.json`, checks that packet against the binary plan and safe summary,
then repeats the account-sensitive guards. The wrapper verifies that:

- the persistent budget has the exact $5 limit, notification set, a subscriber
  on each notification, and only `OK` states;
- the configured Kubernetes version is currently in EKS standard support;
- the plan stays within one EKS cluster, one MSK cluster, one RDS instance,
  one NAT gateway, three maximum Spot workers, 13 topic partitions, and the
  $1.25/hour modelled limit.

The same support check runs again immediately before an hourly apply. A plan
whose binary content changes after review is rejected.

Apply the guardrail stack before the dev plan. In the ignored dev
`terraform.tfvars`, leave all hourly flags false for staging. EKS additionally
requires `eks_operator_cidr` to be the operator's current IPv4 `/32`.

Repositories that still have `mlp-dev-live-runtime` in dev state need one
migration apply. Create and verify `mlp-live-aws-monthly` first, then apply a
reviewed cheap-tier dev plan with all hourly flags false. That plan removes the
old budget while the persistent replacement is already active. The dev module
keeps `budget_alert_email` as a deprecated no-op so an older
`terraform.tfvars` migrates without an undeclared-variable warning.

## Keeping the bill at zero

```bash
make aws-cost    # month-to-date spend
make aws-down    # manual recovery destroy through the initialized backend
```

During a paid run, use `make aws-live-stop` for early cleanup and
`make aws-live-status` to inspect the result. The controller runs state-backed
destroy, removes matching EKS and MSK log groups, requires empty dev Terraform
state, repeats the service-native inventory, and captures immediate cost
output. The inventory includes project-tagged Elastic IPs.

`cleanup_verified: true` means the Terraform state and service inventory are
empty. A failed destroy command, log-deletion call, transcript write, or Cost
Explorer call is recorded separately as `cleanup_complete_with_errors`; it
does not claim that resources remain after the two empty checks pass.

`make aws-down` deliberately does not reconfigure state before a destructive
operation. In a fresh checkout, run `make aws-init` first. After each successful
apply or destroy, Make saves a mode-0600 recovery copy at
`infra/terraform/envs/dev/.terraform/mlp-last-known.tfstate`; the versioned S3
object remains the authority.

If the state bucket is unavailable, stop. Do not run destroy with a local or
disabled backend: Terraform would no longer know which remote resources it
owns. Restore the versioned S3 state first. The private recovery copy is there
to inspect or restore deliberately, not as an automatic fallback that might be
stale.

Find anything this repo left running:

```bash
aws resourcegroupstaggingapi get-resources \
  --tag-filters Key=Project,Values=my-local-platform \
  --profile aws-public-change-feed
```

Every taggable dev-stack resource carries `Project=my-local-platform` and
`Ephemeral=true`. The bootstrap bucket carries the project tag and
`Stack=bootstrap` instead. The query is the first inventory check, not a proof
that nothing else exists: AWS-created EKS log groups are outside Terraform's
tagged resource set.

The bootstrap stack's state bucket has `prevent_destroy = true`. Losing a state
file orphans real infrastructure, which is worse than a fraction of a cent per
month. The persistent cost budget has the same protection.

CloudWatch log groups created by EKS can outlive the cluster. The live
controller deletes project-prefixed EKS and MSK groups before it runs the final
inventory. A manual recovery must do the same.

## A note on billing alerts

For #97, `make aws-cost-final AWS_RUN_ID=<run-id>` collects settled daily
`UnblendedCost` by service over the session's UTC dates, filtered to the
intended account. It requires at least 48 hours after cleanup, all response
pages, every date, and `Estimated: false`. The receipt is validated before
final publication.

The owner accepted this scope on 2026-09-11. Shared-account totals include
unrelated activity and cannot establish exact M4 attribution or audit the
$5 per-run maximum. The foreground controller's deadlines and reviewed
resource shape remain the operational spend controls. See
[the accepted evidence contract](adr/0010-live-aws-relay-contract.md#evidence-and-redaction).

The persistent budget sends email when actual monthly spend passes $4, when it
passes $5, or when forecasted monthly spend passes $5. The account gate refuses
a new hourly plan when any notification is already in `ALARM`.

This is a $5 project monthly allowance before tax. Each session also records a separate
$5 maximum. The $1.25/hour gate and 3-hour hard target cap modeled runtime at
$3.75, leaving $1.25 for small charges outside that model. One run can put
the forecast notification in `ALARM`, which blocks later hourly plans until
AWS reports the notification as `OK` again or the monthly budget period
resets. Treat that block as intentional. Raising the monthly amount requires a
new owner decision.

### Project budget coverage

The owner accepted this scope on 2026-09-20. The exact budget filter is
`TagKeyValue=user:Project$my-local-platform`; tax is explicitly excluded, and
all other cost-type settings retain their existing values. A tag on the budget
resource itself does not define its spending scope.

Terraform applies the project tag to managed resources and passes the same
tags into EKS's launch template for instances, volumes, and network interfaces.
The saved-plan gate checks supported cost-bearing Terraform resources and
these child-resource tags. The live budget check is shared by the account
collector and hourly plan/apply guard. GO requires both receipts, so an old
account-wide receipt cannot satisfy the amended contract.

| Cost | Treatment |
|---|---|
| Tagged project resource usage, including prior runs and surviving resources | Included when AWS allocates the usage to the active project tag |
| Unrelated subscriptions and resources | Excluded by the project filter |
| Tax | Excluded from the monthly alert; review project-related tax separately within the session's existing reserve |
| EKS-created instances and volumes | Explicit launch-template propagation, checked in the plan; verify actual tags during the authorized live run |
| AWS-managed RDS secret and other service-created resources | Inspect actual tags during the live run; account for any unallocated charges separately |
| Untaggable fees, API charges, and public IPv4 charges | Do not assume tag attribution; retain the price reserve and service-native inventory checks |

AWS documents that tagged Elastic IPs do not appear in its cost allocation
report. A passing tag check therefore cannot establish complete bill coverage.
Billing refresh is delayed, and resource tags must have existed at usage time
for historical allocation. The newly activated tag's zero subtotal is not
evidence that earlier project spending was zero. Backfill affects every tag
key's activation history; it is a separate reporting operation, not part of
these guardrail commands.

The final #97 receipt still reports settled account-wide costs with its
existing attribution limitation. A project-filtered Cost Explorer view can
supplement it after billing updates; neither view alone proves exact session
cost or enforces the session clock. Do not generate extra workload to obtain a
nonzero billing sample.

Sources checked on 2026-09-20: [budget filters](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-create-filters.html),
[tag activation](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/activating-tags.html),
[EKS propagation](https://docs.aws.amazon.com/eks/latest/best-practices/cost-opt-awareness.html),
[EC2 billing tags](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html),
and [historical backfill](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/cost-allocation-backfill.html).

AWS Budgets refreshes after billing data arrives, so it cannot enforce M4's
three-hour window. The foreground controller owns the 150-minute destroy
deadline. Its protection depends on the Mac retaining power, network access,
and valid AWS credentials until cleanup passes.

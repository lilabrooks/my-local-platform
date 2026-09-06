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
| ECR repository | $0.10/GB-month | ~$0 |

Leaving these standing costs approximately nothing. The bucket has a 30-day
expiry rule and ECR keeps only the last 10 images, so neither grows unbounded.

## The expensive tier — off by default

| Flag | Creates | Approximate monthly cost |
|---|---|---|
| `enable_rds` | `db.t4g.micro`, 20 GB gp3, single-AZ | **~$15** |
| `enable_eks` | Control plane + 2× `t3.small` spot + NAT gateway | **~$110** |

M4 adds MSK Serverless only behind a separate `enable_msk` flag. The fixed
relay-validation shape in [ADR 0010](adr/0010-live-aws-relay-contract.md) was
rechecked on 2026-09-05 at approximately **$1.02/hour** before small usage
charges. MSK contributes about $0.77/hour of that total. The runbook rejects a
shape above $1.25/hour, starts destroy at 2 hours 30 minutes, ends at 3 hours,
and requires separate approval for a $5 maximum.

Breaking down `enable_eks`, because it is the one that hurts:

- EKS control plane: $0.10/hour = **~$73/month**, charged whether or not a
  single pod is running.
- NAT gateway: ~$0.045/hour = **~$32/month**, plus $0.045/GB processed.
- 2× `t3.small` on spot: **~$6/month** (on-demand would be ~$30).

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
  --query 'clusterVersions[?status==`STANDARD_SUPPORT`].clusterVersion' \
  --profile aws-public-change-feed
```

Local Kubernetes has no such cliff — minikube is free. Use EKS when the goal is
EKS specifically.

Both flags default to `false`.

## Keeping the bill at zero

```bash
make aws-cost    # month-to-date spend
make aws-down    # destroy through the backend initialized by make aws-init
```

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

Two things `terraform destroy` will not clean up, by design:

- The **bootstrap** stack's state bucket has `prevent_destroy = true`. Losing a
  state file orphans real infrastructure, which is worse than a fraction of a
  cent per month.
- **CloudWatch log groups** created by EKS outlive the cluster. Delete them
  manually if they accumulate.

## A note on billing alerts

Nothing in this repository can stop you spending money -- it only makes the
expensive things opt-in and easy to find. A billing alarm is worth setting up
once, in the console, at whatever threshold would annoy you. AWS billing data
does not update fast enough to enforce M4's three-hour window, so the resource
shape, elapsed-time deadline, and destroy rule are the active controls.

# Costs

Everything under `local/` is free. This page is about `infra/terraform/`.

Prices are `us-east-1` list rates as of August 2026 and exclude data transfer.
Treat them as the right order of magnitude, not a quote. The authority on what
you are actually spending is `make aws-cost`.

## The cheap tier — created by default

| Resource | Billing | Idle cost |
|---|---|---|
| S3 bucket | $0.023/GB-month | ~$0 |
| SNS topic | $0.50 per million publishes, first million free | ~$0 |
| SQS queue + DLQ | $0.40 per million requests, first million free | ~$0 |
| SES identity | $0.10 per thousand emails | ~$0 |
| ECR repository | $0.10/GB-month | ~$0 |

Leaving these standing costs approximately nothing. The bucket has a 30-day
expiry rule and ECR keeps only the last 10 images, so neither grows unbounded.

## The expensive tier — off by default

| Flag | Creates | Approximate monthly cost |
|---|---|---|
| `enable_rds` | `db.t4g.micro`, 20 GB gp3, single-AZ | **~$15** |
| `enable_eks` | Control plane + 2× `t3.small` spot + NAT gateway | **~$110** |

Breaking down `enable_eks`, because it is the one that hurts:

- EKS control plane: $0.10/hour = **~$73/month**, charged whether or not a
  single pod is running.
- NAT gateway: ~$0.045/hour = **~$32/month**, plus $0.045/GB processed.
- 2× `t3.small` on spot: **~$6/month** (on-demand would be ~$30).

Both flags default to `false`.

## Keeping the bill at zero

```bash
make aws-cost    # month-to-date spend
make aws-down    # destroy the dev environment
```

Find anything this repo left running:

```bash
aws resourcegroupstaggingapi get-resources \
  --tag-filters Key=Project,Values=my-local-platform \
  --profile aws-public-change-feed
```

Every resource Terraform creates here is tagged `Project=my-local-platform`
and `Ephemeral=true`, so that query is exhaustive.

Two things `terraform destroy` will not clean up, by design:

- The **bootstrap** stack's state bucket has `prevent_destroy = true`. Losing a
  state file orphans real infrastructure, which is worse than a fraction of a
  cent per month.
- **CloudWatch log groups** created by EKS outlive the cluster. Delete them
  manually if they accumulate.

## A note on billing alerts

Nothing in this repository can stop you spending money — it only makes the
expensive things opt-in and easy to find. A billing alarm is worth setting up
once, in the console, at whatever threshold would annoy you.

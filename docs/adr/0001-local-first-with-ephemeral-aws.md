# 1. Local-first, with real AWS as an opt-in ephemeral layer

Date: 2026-08-23
Status: Accepted

## Context

The goal is learning and exploration across Kafka, RabbitMQ, Datadog, EKS, SNS,
SES, S3 and RDS, plus showcase applications on a public GitHub profile. The AWS
account is a personal one, accessed through SSO with `AdministratorAccess`.
The account id is deliberately not recorded here -- this repository is public,
and an account id is enough to enumerate roles against.

An always-on version of this stack on a personal account is not cheap. Priced
in `us-east-1` at list rates: an EKS control plane is $0.10/hour (~$73/month), a
NAT gateway is ~$32/month plus data processing, `db.t4g.micro` RDS is ~$15/month,
and each ALB adds ~$16/month. A permanently running environment lands somewhere
around $150-250/month. That is a poor trade for a learning environment that is
idle most of the time.

Meanwhile, Docker on this machine has ~8 GB and 10 CPUs available, which is
enough for the whole local stack if components are grouped rather than all run
at once.

## Decision

Everything runs locally by default. Real AWS is opt-in, ephemeral, and split by
cost:

- **Local (free, always available):** Kafka, RabbitMQ, Postgres, the AWS API
  surface via floci, and the observability stack.
- **Real AWS, cheap tier (~$0/month idle):** S3, SNS, SQS, SES, ECR. Serverless
  and pay-per-request, safe to leave standing.
- **Real AWS, expensive tier (off by default):** EKS and RDS, behind
  `enable_eks` and `enable_rds` Terraform variables, both defaulting to `false`.

Everything Terraform creates is tagged `Project=my-local-platform` and
`Ephemeral=true`, so anything left running can be found in one query.

## Consequences

Fast iteration and no surprise bills. The cost of that is fidelity: local
emulation is not AWS, and IAM in particular behaves differently. Anything that
depends on real IAM semantics, real SES sending limits, or real EKS networking
has to be verified against the cheap tier or a temporary expensive-tier apply.

`make aws-cost` reports month-to-date spend, and `make aws-down` destroys the
environment. The discipline this relies on is remembering to run the latter.

## Verification

`terraform plan` with both flags enabled resolves to 65 resources; the default
plan is 10, all serverless.

The cheap tier has been **applied to the real account and destroyed again**, so
it is verified rather than merely planned:

```text
Apply complete! Resources: 10 added, 0 changed, 0 destroyed.
Destroy complete! Resources: 10 destroyed.
```

Afterwards `aws resourcegroupstaggingapi get-resources --tag-filters
Key=Project,Values=my-local-platform` returns 0, confirmed independently by
listing S3, SNS, SQS and ECR by name. Total cost of the exercise was within the
free tier.

The expensive tier (`enable_rds`, `enable_eks`) remains plan-only.

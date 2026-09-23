# M4 #97 execution preparation, 2026-09-22

Status: this preparation preceded owner approval and the failed 2026-09-22
paid attempt. Its plan and observations are historical, not authority for a
retry. See the [attempt record](m4-97-live-attempt-20260922.md) for execution,
cleanup and the required repaired candidate. At the preparation boundary,
no hourly apply, live capture, session receipt or controller state existed.

## Candidate and preserved evidence

- Issue: [#97](https://github.com/lilabrooks/my-local-platform/issues/97).
- Runtime source: `474dca7e8e121c08f2951b02871cfe4c4b87e2ee`.
- Qualified local run: `20260920T153931Z`; its existing qualification was retained.
- AWS staging/session ID: `20260920T155738Z`, still unused for paid execution.
- Execution checkout: `/private/tmp/mlp-m4-96-474dca7`, detached at the qualified
  source. Publication and this review live outside that checkout.

Before refreshing any receipt, all 39 files in the candidate's staging directory
were copied to `.evidence/m4-archives/20260920T155738Z-before-97-20260922T173256Z/`.
Every copied file's SHA-256 matched; `archive-sha256.json` records those hashes.
The original published #96 packet and the main checkout's raw staging archive
were left intact. The preflight and capture-plan bytes are unchanged. Both ECR
digests match the historical staging receipt.

## Refreshed checks

The following commands ran from the qualified checkout with
`AWS_RUN_ID=20260920T155738Z`, the full source SHA as `AWS_APPROVED_COMMIT`,
profile `aws-public-change-feed`, and region `us-east-1`:

| Check | Result |
|---|---|
| `make aws-account-check` | Passed at 17:33:16Z: intended SSO account and AdministratorAccess role, backend controls, project budget, quotas and regional availability |
| EKS version from account check | 1.35, `STANDARD_SUPPORT` |
| AWS Price List `get-products`, eight existing SKUs | All eight rates unchanged; original AWS pricing pages also reviewed |
| `make aws-prices` | Passed; price review at 17:34:19Z, modeled total $1.0218506849/hour |
| `make aws-inspect-images` | Passed; immutable tags, original digests, `linux/amd64`, exact source revision |
| `make aws-inventory-empty` | Passed at 17:34:47Z; two ECR repositories, no matching hourly runtime |
| `make aws-plan` with EKS/MSK/RDS enabled and current operator IPv4 `/32` | Passed; summary at 17:35:22Z; 82 creates, no managed-resource updates or deletes |
| `make aws-go-no-go M4_OPERATOR=lilabrooks` | Passed at 17:36:11Z |
| `m4-stage.py verify-go-no-go --before-session` | Passed, including exact saved-plan hash, clean candidate, current operator IPv4 and 15-minute freshness reserve |

Read-only account inspection earlier in this task found the project budget's
reported actual spend at $0 against $5, with all three notifications OK. Shared
account September cost through September 21 was $33.2143 and still estimated;
that amount is not a measured M4 session cost.

Private command logs and fresh raw receipts are under the candidate's
`.evidence/m4/20260920T155738Z/`. Fresh Price List responses are in
`97-pricing-sources/`; the complete plan JSON is `97-reviewed-plan-private.json`.
These include private account data and must not be committed.

## Reviewed apply

Saved-plan SHA-256:

```text
d2adcee6c8fe226eae99bdd719662dcbcb3ec33fe2b4efc1011f5d2845bf64c7
```

The fixed topology remains one EKS 1.35 cluster, two desired Spot `t3.medium`
workers (range one to three), one MSK Serverless cluster with 12 delivery
partitions and one DLQ partition, one private PostgreSQL 17.11
`db.t4g.micro` with 20 GB gp3, and one NAT gateway. The plan includes the
supporting network and IAM resources. RDS is encrypted and uses a managed
master password. The EKS public API is restricted to the verified operator
IPv4 `/32`; no globally open explicit ingress rule was found in the plan.
The plan's project and EKS child-resource tag checks passed.

The price model is unchanged and remains below the $1.25/hour shape limit.
It is a model, not a final bill or an instantaneous spending cutoff. The
session maximum remains $5. AWS pricing sources checked:
[MSK](https://aws.amazon.com/msk/pricing/),
[EKS](https://aws.amazon.com/eks/pricing/),
[EC2](https://aws.amazon.com/ec2/pricing/on-demand/),
[VPC](https://aws.amazon.com/vpc/pricing/), and
[RDS PostgreSQL](https://aws.amazon.com/rds/postgresql/pricing/).

## Execution and closure boundary

The next action requires separate owner permission for this reviewed hourly
apply, bounded capture and full dev teardown. The controller starts destroy
at 150 minutes and has a 180-minute hard deadline. Successful capture requests
cleanup promptly; divergence or failure requests cleanup immediately. No
paid debugging or retry is included. Cleanup includes staged ECR repositories;
the remote-state bootstrap and persistent budget remain separate.

Use `make aws-live-stop AWS_RUN_ID=20260920T155738Z` to request controller
cleanup. Follow the existing runbook's recovery procedure if controller cleanup
fails. Keep the Mac powered, open and online until cleanup is verified.

Before execution, repeat pre-session verification. A changed operator IP or
expired observation requires the corresponding refresh, plan/GO review and
permission for the resulting plan. The refreshed inputs have 24-hour age
limits; the controller reserves 15 minutes before starting.

Even a successful live run leaves #97 open until the evidence is exported and
sanitized, dev teardown and inventories pass, the actual duration is recorded,
and settled billing evidence is captured at least 48 hours after cleanup with
every covered date no longer estimated. Update the ADR and README against
those results before claiming M4 complete.

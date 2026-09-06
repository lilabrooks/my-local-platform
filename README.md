# my-local-platform

[![CI](https://github.com/lilabrooks/my-local-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/lilabrooks/my-local-platform/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A local-first playground for building cloud applications. Build and test on a
laptop, then validate briefly on real AWS through an explicit, short-lived
Terraform workflow.

The default workflow uses Docker Compose and minikube. It combines an
AWS-compatible local API with Kafka, RabbitMQ, Postgres, GitOps, and a complete
telemetry path. A separate Terraform workflow creates real AWS resources when
local evidence is no longer enough. Paid resources are opt-in, tagged, and
designed to be destroyed after the test.

`relay`, a webhook delivery service, is the first application built on the
platform. It is the current reference path across local services, Kubernetes,
observability, and smoke tests. M4 extends that path to AWS.

## What is included

| Area | Local path | AWS path |
|---|---|---|
| AWS APIs | floci for S3, SNS, SQS, SES, and container-backed services | S3, SNS, SQS, ECR, optional SES, RDS, and EKS |
| Streaming | Apache Kafka in KRaft mode | MSK Serverless with IAM for the live relay milestone |
| Messaging | RabbitMQ | Amazon MQ is a counterpart; Terraform does not provision it |
| Database | Postgres 18 | RDS for PostgreSQL, opt-in |
| Kubernetes | dedicated `mlp` minikube profile | EKS, opt-in |
| Deployment | ArgoCD app-of-apps | the same project boundary with immutable ECR image digests |
| Telemetry | OpenTelemetry, Prometheus, Tempo, and Grafana | OpenTelemetry with an optional Datadog exporter |

Everything in `local/` runs without an AWS account. Terraform under
`infra/terraform/` is the boundary where real cloud credentials and costs
begin.

## Quick start

The full local stack needs Docker, Docker Compose, Go 1.27 or newer, and Make.
It uses about 1.6 GB of sustained memory after startup. The first run can take a
few minutes while Docker downloads and builds images.

```bash
cp .env.example .env
make up
make smoke
```

`make up` starts every Compose profile and seeds the local AWS resources,
Kafka topics, and relay database. `make smoke` writes and reads back through
S3, SNS to SQS, SES, Kafka, RabbitMQ, Postgres, and relay. A passing run ends
with:

```text
all components healthy
```

Print the service URLs with `make urls`. Stop the stack while keeping its data
with `make down`. `make clean` also deletes the local volumes.

See the [local runbook](docs/runbook-local.md) for ports, credentials, memory
measurements, tracing, and troubleshooting.

## Run only what you need

Compose profiles keep smaller experiments light.

| Command | Starts |
|---|---|
| `make up-core` | floci and Postgres |
| `make up-messaging` | Kafka and RabbitMQ |
| `make up-tools` | Kafka UI plus the messaging profile |
| `make up-obs` | OpenTelemetry Collector, Prometheus, Tempo, and Grafana |
| `make up-apps` | relay and its test sink, plus core and messaging |
| `make up` | every profile |

## Build another application

The repository is meant to hold more than one cloud application. `relay` shows
the current application seam:

1. Put application code in `services/<name>/`.
2. Add its local runtime and dependencies to `local/docker-compose.yml`.
3. Add repeatable resource setup under `local/bootstrap/`.
4. Put Kubernetes resources in `k8s/manifests/<name>/` and register an ArgoCD
   `Application` in `k8s/apps/`.
5. Add a smoke check that writes data, reads it back, and asserts the result.
6. Add Terraform only for the cloud behavior that needs a live AWS check.

This split keeps application work cheap and repeatable. The live AWS step has a
narrow purpose and an explicit teardown path.

## The first application: relay

`relay` accepts tenant events, stores their history in Postgres, writes them to
Kafka, and delivers signed webhooks through a consumer group. It has
idempotent ingest, ordered delivery per tenant, retries, dead-letter handling,
replay, and Prometheus metrics. KEDA scales the delivery workers from broker
lag.

```bash
make up-apps
make seed
make smoke
```

The [relay goal](docs/goal-relay.md) defines its behavior and limitations. The
[relay roadmap](docs/roadmap-relay.md) records the milestone sequence and the
evidence behind it.

Current state:

- M0 through M3 are complete. The M3 whole-application proof passed locally on
  2026-09-05, and
  [issue #90](https://github.com/lilabrooks/my-local-platform/issues/90) is
  closed.
- [Issue #91](https://github.com/lilabrooks/my-local-platform/issues/91) closed
  with the accepted M4 contract. Its topology, identity, three-hour paid
  window, $5 maximum, evidence set, and mandatory destroy path are in
  [ADR 0010](docs/adr/0010-live-aws-relay-contract.md) and the
  [AWS relay runbook](docs/runbook-aws-relay.md).
- [Issue #92](https://github.com/lilabrooks/my-local-platform/issues/92) is
  complete; it added the locally tested MSK IAM transport. #93 and #101 are the
  remaining parallel M4 foundation work before the rendered deployment in #94.
  The later AWS run remains rehearsed, short-lived, and separately authorized.
- The live S3 and SNS-to-SQS path has already been tested and destroyed. EKS,
  RDS, and MSK validation remains future work.

Run `make relay-demo` after completing the cluster setup in the
[Kubernetes runbook](docs/runbook-k8s.md). The measured results and the choice
to scale on lag are recorded in
[ADR 0007](docs/adr/0007-keda-lag-autoscaling.md).

## Kubernetes and GitOps

The local cluster uses a dedicated minikube profile named `mlp`. It does not
reuse another Kubernetes context on the machine.

```bash
make k8s-up
make echo-image
make argocd-install
make k8s-status
```

ArgoCD watches `k8s/apps/` and deploys each workload from git. The root
Application can create child Applications in `argocd`; workload Applications
can deploy only into the `mlp` namespace. The built-in `default` project has no
deployment permissions.

A fork must point the root and child Applications at the fork's git URL. The
[Kubernetes runbook](docs/runbook-k8s.md) covers public and private remotes,
image loading, KEDA, the in-cluster Grafana stack, and the relay demo.

## Brief AWS validation

Real AWS requires Terraform 1.10 or newer, AWS CLI v2, and an AWS SSO session.
Read the [cost guide](docs/costs.md) before running any apply.
The [AWS relay runbook](docs/runbook-aws-relay.md) adds the stricter contract
for M4. Its contract, cheap staging, and hourly apply require separate owner
decisions.

```bash
make aws-login
make aws-whoami
make aws-bootstrap  # once per account
make aws-init
make aws-plan
```

The default plan creates the low-cost tier: S3, SNS, SQS, and ECR, with SES
enabled only when a sender address is supplied. `enable_rds` and `enable_eks`
both default to `false` because they create hourly charges.

After reviewing the plan and deciding to create the resources:

```bash
make aws-up
```

Finish the session by destroying the dev stack and checking the account's
month-to-date spend:

```bash
make aws-down
make aws-cost
```

The versioned S3 state bucket is a separate bootstrap resource and survives a
dev-stack destroy. The cost guide explains the current estimates, state
recovery, tags, and resources that AWS may leave behind.

## Repository layout

```text
local/                 Docker Compose stack, profiles, and seed scripts
services/
  smoke/               end-to-end round-trip checks
  echo/                small HTTP workload used by the GitOps path
  relay/               webhook ingest and delivery service
  sink/                controllable relay subscriber for tests and demos
k8s/
  argocd/              ArgoCD install, projects, and root Application
  apps/                one ArgoCD Application per workload
  manifests/           Kubernetes workload resources
infra/terraform/
  bootstrap/           account-scoped remote state bucket
  envs/dev/            low-cost resources and opt-in RDS/EKS
docs/adr/               decisions with commands and measured evidence
```

## Design and evidence

The architecture decisions explain why the repository uses this shape:

| Decision | Status |
|---|---|
| [Local-first with ephemeral AWS](docs/adr/0001-local-first-with-ephemeral-aws.md) | Accepted |
| [floci over LocalStack](docs/adr/0002-floci-over-localstack.md) | Accepted |
| [OpenTelemetry-first observability](docs/adr/0003-otel-first-observability.md) | Accepted |
| [Real Kafka for local development](docs/adr/0004-real-kafka-not-emulated.md) | Accepted |
| [ArgoCD for GitOps](docs/adr/0005-argocd-gitops.md) | Accepted |
| [Kafka for relay delivery](docs/adr/0006-kafka-over-sqs-for-delivery.md) | Accepted |
| [KEDA scaling from consumer lag](docs/adr/0007-keda-lag-autoscaling.md) | Accepted |
| [In-cluster observability for the demo](docs/adr/0008-in-cluster-observability-for-the-demo.md) | Accepted |
| [Separate ArgoCD control and workload permissions](docs/adr/0009-separate-argocd-control-and-workload-projects.md) | Accepted |
| [Live AWS relay validation contract](docs/adr/0010-live-aws-relay-contract.md) | Accepted |

GitHub Issues and milestones hold the active backlog. Start with the
[open issues](https://github.com/lilabrooks/my-local-platform/issues) for the
current work and [AGENTS.md](AGENTS.md) for repository rules and cost
guardrails.

## Verification

Run the checks that cover your change:

```bash
make test          # Go tests across all modules
make k8s-validate  # manifest and GitOps invariants
make lint          # Go, YAML, shell, Markdown, docs, Actions, Docker, Terraform, security, secrets
```

`make smoke` is the local end-to-end gate and needs the Compose stack running.
CI runs the same tests, linters, image builds, Terraform validation, and smoke
path on every pull request.

See [Repository file checks](docs/repository-file-checks.md) for the full local
and CI inventory, including security scans, ArgoCD invariants, and runtime
verification.

## License

Licensed under the [Apache License 2.0](LICENSE).

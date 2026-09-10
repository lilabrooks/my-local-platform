# my-local-platform

[![CI](https://github.com/lilabrooks/my-local-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/lilabrooks/my-local-platform/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A local-first platform built primarily for day-to-day development and fast
testing on a laptop. Docker Compose provides Kafka, RabbitMQ, Postgres, an
AWS-compatible API, and an OpenTelemetry stack. A dedicated minikube profile
adds the GitOps path.

Live AWS is reserved for brief validation runs when local evidence cannot
answer an AWS-specific question. A guarded, short-lived Terraform workflow
handles that path. Every hourly resource is disabled by default and needs an
explicit owner decision before creation. `relay`, a webhook delivery service,
is the reference application across Compose, minikube, and live AWS.

## Table of contents

- [Quick start](#quick-start)
- [What runs here](#what-runs-here)
- [Relay](#relay)
- [Kubernetes and GitOps](#kubernetes-and-gitops)
- [Real AWS](#real-aws)
- [Project documentation](#project-documentation)
- [Design and evidence](#design-and-evidence)
- [Development](#development)
- [License](#license)

## Quick start

The Compose path needs Docker with the Compose plugin, Go 1.27 or newer,
Python 3, and GNU Make. Start Docker first, then run:

```bash
make up
make smoke
```

`make up` builds the local applications, starts every Compose profile, and
seeds the local AWS-compatible resources, Kafka topics, and relay database.
`make smoke` writes and reads back through S3, SNS to SQS, SES, Kafka,
RabbitMQ, Postgres, and relay. These commands need no AWS account or
credentials. A passing run ends with:

```text
all components healthy
```

The defaults load from `.env.example`. Copy it to the ignored `.env` file only
when you need local overrides:

```bash
cp .env.example .env
```

Run `make urls` to print service addresses. `make down` stops the stack and
keeps its volumes; `make clean` stops it and deletes all local data.

> **Host capacity:** Kubernetes is the resource-bound path. On 2026-09-08, the
> current development host was a MacBook Air (`Mac16,12`, Apple M4, 10 cores,
> 16 GB, arm64) running macOS 26.6.2, with Docker assigned 10 CPUs and about
> 8 GB. A minimum host has not been measured. Repository measurements put all
> Compose profiles together at about 1.6 GB under sustained load. The
> Kubernetes test used a 4 CPU, 6 GiB minikube node; the same workload failed
> with 3 GiB. See the
> [measured memory breakdown](docs/runbook-local.md#profiles).

## What runs here

| Area | Local path | AWS path |
|---|---|---|
| AWS APIs | floci for S3, SNS, SQS, SES, and related APIs | S3, SNS, SQS, ECR, optional SES, RDS, and EKS |
| Streaming | Apache Kafka in KRaft mode | MSK Serverless with IAM for the live relay proof |
| Messaging | RabbitMQ | Amazon MQ is outside the current Terraform scope |
| Database | Postgres 18 | RDS for PostgreSQL, opt-in |
| Kubernetes | dedicated `mlp` minikube profile | EKS, opt-in |
| Deployment | ArgoCD app-of-apps | the same project boundaries with ECR image digests |
| Telemetry | OpenTelemetry Collector, Prometheus, Tempo, Grafana, and optional Datadog export | OpenTelemetry Collector, Prometheus, Tempo, and Grafana |

Compose and minikube run without an AWS account. Real credentials and costs
begin under `infra/terraform/`.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Compose profiles and their commands</summary>

| Command | Starts |
|---|---|
| `make up-core` | floci and Postgres |
| `make up-messaging` | Kafka and RabbitMQ |
| `make up-tools` | Kafka UI plus the messaging profile |
| `make up-obs` | OpenTelemetry Collector, Prometheus, Tempo, and Grafana |
| `make up-apps` | relay and its test sink, plus core and messaging |
| `make up` | every profile |

The [local runbook](docs/runbook-local.md) has ports, credentials, tracing,
profile measurements, and troubleshooting.

</details>
<!-- markdownlint-enable MD033 -->

## Relay

`relay` accepts tenant events, records their history in Postgres, writes them
to Kafka, and delivers signed webhooks through a consumer group. It supports
idempotent ingest, ordered delivery per tenant, bounded retries, dead letters,
replay, metrics, and KEDA scaling from broker lag.

Run the application path without the observability profile:

```bash
make up-apps
make seed
make smoke
```

M0 through M3 and the locally testable M4 implementation are complete,
including the one-shot runtime bootstrap packaged in
[issue #136](https://github.com/lilabrooks/my-local-platform/issues/136). The
live EKS, RDS, and MSK proof remains unverified and separately authorized.
[Issue #96](https://github.com/lilabrooks/my-local-platform/issues/96) tracks
staging; [issue #97](https://github.com/lilabrooks/my-local-platform/issues/97)
tracks the paid run.

The [relay goal](docs/goal-relay.md) states the behavior and limits. The
[relay roadmap](docs/roadmap-relay.md) records the milestone sequence and its
evidence.

## Kubernetes and GitOps

Local Kubernetes uses the minikube profile `mlp`, which keeps it separate from
other contexts. ArgoCD's root application can create child applications in
`argocd`; workload applications can deploy only into the `mlp` namespace. The
built-in `default` project has no deployment permissions.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Start the local GitOps path</summary>

This path also needs minikube, kubectl, and Helm:

```bash
make k8s-up
make echo-image
make argocd-install
make k8s-status
```

ArgoCD reads committed manifests from a git remote. A fork must supply its own
URL with `REPO_URL`; a private fork also needs the read-only deploy-key setup.
The [Kubernetes runbook](docs/runbook-k8s.md) covers both cases, image loading,
KEDA, monitoring, and the relay demo.

</details>
<!-- markdownlint-enable MD033 -->

## Real AWS

Use live AWS for brief, focused validation after the local checks pass. The M4
controller starts destroy at 2 hours 30 minutes and treats cleanup beyond 3
hours as a failed deadline. It keeps destroying if AWS cleanup runs long.
`make aws-live-run` owns that clock, stays in the foreground, and enters
cleanup after success, failure, interruption, or the destroy deadline. If
Terraform is still applying at that deadline, the controller interrupts it
before cleanup.

The AWS path needs Terraform 1.10 or newer, AWS CLI v2, `jq`, Go 1.27 or
newer, Python 3, and an AWS SSO session. The default Terraform tier contains
usage-priced resources with near-zero idle cost. `enable_rds`, `enable_eks`,
and `enable_msk`
default to `false` because they create hourly charges.

Read the [cost and teardown guide](docs/costs.md) first. The
[AWS relay runbook](docs/runbook-aws-relay.md) owns the identity checks, guarded
plan, separate authorization gates, evidence capture, abort path, and final
resource audit. No real-AWS command is part of the local quick start.

The live controller uses macOS `caffeinate` when available. Mac model, CPU, and
memory do not control the AWS duration; host availability does. Keep the Mac
powered, awake, open, and online until `make aws-live-status` reports verified
cleanup. A local controller cannot act through a power, network, or credential
failure.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Terraform scope and safety boundary</summary>

- The cheap tier contains S3, SNS, SQS, 2 ECR repositories, and optional SES.
- A separate persistent stack owns the account-wide $5 monthly AWS Budget, so
  dev cleanup cannot delete the alert.
- The live relay proof adds EKS, RDS, and MSK Serverless only when their flags
  are enabled.
- `make aws-plan` saves an exact plan and a redaction-safe summary.
- An hourly `make aws-up` refuses to run outside `make aws-live-run`.
- `make aws-runtime-bootstrap` requires that live controller, verifies the EKS
  context, and runs the idempotent MSK and RDS setup before workloads start.
- `make aws-down` destroys the dev stack. The versioned remote-state bucket is
  a separate bootstrap resource; the state bucket and cost alert survive by
  design.

</details>
<!-- markdownlint-enable MD033 -->

## Project documentation

| Need | Document |
|---|---|
| Run or troubleshoot Compose | [Local runbook](docs/runbook-local.md) |
| Run minikube and ArgoCD | [Kubernetes runbook](docs/runbook-k8s.md) |
| Prepare the M4 AWS session | [AWS relay runbook](docs/runbook-aws-relay.md) |
| Understand AWS costs and teardown | [Cost guide](docs/costs.md) |
| Read the relay contract and sequence | [Goal](docs/goal-relay.md) and [roadmap](docs/roadmap-relay.md) |
| Inspect local and CI checks | [Repository file checks](docs/repository-file-checks.md) |
| Follow repository rules | [AGENTS.md](AGENTS.md) |

GitHub Issues and milestones hold the active backlog. The
[open issues](https://github.com/lilabrooks/my-local-platform/issues) are the
current work queue.

## Design and evidence

Each architecture decision records its verification evidence.

<!-- markdownlint-disable MD033 -->
<details>
<summary>Architecture decision index</summary>

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

</details>
<!-- markdownlint-enable MD033 -->

## Development

Run the checks that cover your change:

```bash
make test          # race-enabled tests across 6 Go modules, then Python tests
make lint          # source, docs, infrastructure, security, and secret checks
make k8s-validate  # manifest tests and schema validation; no cluster or AWS
```

`make k8s-validate` needs Docker, Helm, and kubectl. Its first run downloads
pinned validation inputs. `make smoke` is the local round-trip gate and needs
the Compose stack running. CI runs the same core checks for pull requests; its
full job inventory is in [Repository file checks](docs/repository-file-checks.md).

| Path | Contents |
|---|---|
| `local/` | Compose stack, profiles, configuration, and seed scripts |
| `services/` | smoke checks, echo, relay, and the test sink |
| `k8s/` | ArgoCD projects, application registrations, overlays, and validation |
| `infra/terraform/` | account bootstrap, persistent guardrails, and the guarded dev stack |
| `docs/` | runbooks, goal and roadmap documents, ADRs, and evidence |

<!-- markdownlint-disable MD033 -->
<details>
<summary>Add another application</summary>

1. Put its code in `services/<name>/`.
2. Add its local runtime and dependencies to `local/docker-compose.yml`.
3. Add repeatable setup under `local/bootstrap/`.
4. Put Kubernetes resources in `k8s/manifests/<name>/` and register an ArgoCD
   `Application` in `k8s/apps/`.
5. Add a smoke check that writes data, reads it back, and asserts the result.
6. Add Terraform only for behavior that needs a live AWS check.

</details>
<!-- markdownlint-enable MD033 -->

## License

Licensed under the [Apache License 2.0](LICENSE).

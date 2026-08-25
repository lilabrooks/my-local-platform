# my-local-platform

A distributed-systems playground that runs on a laptop for free, and on real
AWS when it needs to.

Kafka, RabbitMQ, Postgres, S3, SNS, SQS, SES, OpenTelemetry, Prometheus, Tempo
and Grafana come up with one command and no cloud account. ArgoCD deploys onto
a local Kubernetes cluster from git. EKS and RDS are one Terraform flag away
when emulation is not enough.

```bash
cp .env.example .env
make up
make smoke
```

```text
smoke check  aws=http://localhost:4566 region=us-east-1 kafka=localhost:9092

  PASS  s3               18ms  s3://mlp-artifacts/smoke/1787624424137397000.txt round trip
  PASS  sns->sqs         16ms  fanout delivered smoke-1787624424165875000
  PASS  ses               4ms  sent message 3254d6c8-84f5-4225-ab89-6e88607344ed
  PASS  kafka           211ms  mlp.events partition 0 offset 6
  PASS  rabbitmq         14ms  queue mlp.smoke round trip
  PASS  postgres         17ms  row 6 on postgres 18.6

all components healthy
```

## What is here

```text
local/          docker-compose stack, split into profiles
  bootstrap/    idempotent seed scripts for AWS resources and Kafka topics
  config/       OTel Collector, Prometheus, Tempo, Grafana provisioning
services/
  smoke/        Go service that writes to and reads back from every component
  echo/         small HTTP service, the workload ArgoCD deploys
k8s/
  argocd/       ArgoCD install, AppProject, root Application
  apps/         one Application per workload (app-of-apps)
  manifests/    what those Applications point at
infra/terraform/
  bootstrap/    remote state backend, run once
  envs/dev/     the AWS environment, split cheap vs expensive
docs/adr/       why each choice was made, and what was verified
```

## The stack

| Component | Local | Real AWS |
|---|---|---|
| Object storage | floci S3 | S3 |
| Container-backed AWS | floci RDS/EKS (opt-in socket) | the real services |
| Pub/sub + queues | floci SNS/SQS | SNS + SQS with a DLQ |
| Email | floci SES | SES |
| Event streaming | Apache Kafka (KRaft) | MSK, or self-managed |
| Message broker | RabbitMQ | Amazon MQ |
| Relational | Postgres 17 | RDS (`enable_rds`) |
| Kubernetes | minikube (`mlp` profile) | EKS (`enable_eks`) |
| Deployment | ArgoCD, app-of-apps | same manifests, ECR images |
| Telemetry | OTel → Prometheus + Tempo + Grafana | OTel → Datadog |

## Design decisions

Six choices shape this repository, each recorded with the evidence behind it:

- **[Local-first, ephemeral AWS](docs/adr/0001-local-first-with-ephemeral-aws.md)** —
  an always-on version of this stack runs ~$150-250/month on a personal
  account. Expensive resources are opt-in and tagged so they can be found.
- **[floci over LocalStack](docs/adr/0002-floci-over-localstack.md)** —
  LocalStack retired its free Community Edition in 2026 and now requires an auth
  token. A public repository should clone and run.
- **[OpenTelemetry-first](docs/adr/0003-otel-first-observability.md)** —
  Datadog is one exporter behind a collector, not a dependency. Application
  code names no vendor.
- **[Real Kafka, not emulated MSK](docs/adr/0004-real-kafka-not-emulated.md)** —
  the goal is learning Kafka, not learning MSK's control plane.
- **[ArgoCD for GitOps](docs/adr/0005-argocd-gitops.md)** — pull-based
  deployment onto a dedicated minikube profile, so an existing cluster on the
  machine is left alone.

- **[Kafka over SNS to SQS for delivery](docs/adr/0006-kafka-over-sqs-for-delivery.md)** —
  webhook delivery needs replay, and a queue deletes on acknowledgement. Cost is
  what argues the other way, and an ephemeral stack never pays it.

That last one covers `relay`, the first application — see
**[its goal](docs/goal-relay.md)** and **[roadmap](docs/roadmap-relay.md)**.
No code is written yet.

## The smoke service

`services/smoke` is the load-bearing piece of this repository. Every check
**writes something and reads it back**, then asserts the payload matches — a
check that merely opens a connection proves a port is listening, which is not
the same as the component working. It exits non-zero on failure, so it doubles
as a CI gate.

It is also the reference for how to talk to each component: the AWS SDK against
a custom endpoint, a Kafka producer and consumer group, an AMQP round trip,
`pgx`, and OTLP tracing that wraps every check in a span.

## Kubernetes and GitOps

```bash
make k8s-up          # dedicated 'mlp' minikube profile
make echo-image      # build and load the workload image
make argocd-install  # ArgoCD + app-of-apps
make argocd-ui       # https://localhost:8081
```

ArgoCD pulls from a git URL. This repository is private, so ArgoCD needs a
read-only deploy key before it can sync — `make argocd-repo-creds` generates
one, registers it, and repoints the Applications at the SSH URL.
`make k8s-apply-local` applies the same manifests directly, without git.
**[docs/runbook-k8s.md](docs/runbook-k8s.md)** covers the details.

## Real AWS

Requires an SSO session:

```bash
make aws-login      # aws sso login
make aws-whoami
make aws-plan       # free, read-only
make aws-up         # prompts before creating anything billable
make aws-cost       # month-to-date spend
make aws-down       # destroy
```

The default environment is ten serverless resources that cost approximately
nothing idle. EKS and RDS are behind `enable_eks` and `enable_rds`, both
`false` by default. **[docs/costs.md](docs/costs.md)** has the arithmetic.

## Linting

```bash
make lint
```

Ten checks: Go, YAML, shell, Markdown, GitHub Actions workflows, the
Dockerfile, Terraform formatting and lint rules, infrastructure security, and a
secret scan across git history.

Each linter uses a local binary when one is installed and a pinned container
otherwise, so it works on a clean machine with only Docker. A linter that can
run neither way reports `SKIP` rather than passing silently.

| Check | Tool | Catches |
|---|---|---|
| YAML | yamllint | syntax errors, indentation |
| Shell | shellcheck | quoting bugs, unsafe `cd`, bad assignments |
| Markdown | markdownlint | broken structure, unlabelled code fences |
| Actions | actionlint | workflow errors that otherwise appear only on push |
| Docker | hadolint | Dockerfile antipatterns |
| Go | golangci-lint | unchecked errors, dead code, staticcheck |
| Terraform | fmt + tflint | formatting, deprecated syntax, invalid values |
| Infra security | trivy | encryption, CVEs, misconfiguration |
| Secrets | gitleaks | credentials committed to history |

## Agent tooling

The repository declares its own MCP servers in `.mcp.json`, so a fresh clone
gets the same code-intelligence tooling rather than depending on whatever each
machine happens to have configured:

| Server | Transport | Use |
|---|---|---|
| `codegraph` | stdio | symbol source, call paths, blast radius (`.codegraph/` is indexed) |
| `semble` | stdio | semantic search when the symbol name is unknown |
| `token-savior` | stdio | change impact, affected tests, routes, env usage |
| `parallel-search` | http | web search and page fetch |

`.claude/launch.json` adds attach-only browser targets for the stack's web UIs,
so an agent can open Grafana or the ArgoCD UI against a running stack.

**[AGENTS.md](AGENTS.md)** carries the working rules — cost guardrails, which
lookup tool to reach for, how to verify a change, and the traps that have
already caught someone. `CLAUDE.md` points at it so there is one source of
truth rather than per-agent copies.

## Requirements

Docker, Go 1.27+, Terraform 1.9+, the AWS CLI v2, minikube and kubectl.

The linters need only Docker, and fall back to pinned containers for anything
not installed. Two are worth having natively because they run on every change:

```bash
brew install trivy golangci-lint
```

`make help` lists every target. **[docs/runbook-local.md](docs/runbook-local.md)**
covers ports, credentials and troubleshooting;
**[docs/runbook-k8s.md](docs/runbook-k8s.md)** covers the cluster.

## Status

The local stack and the smoke checks are verified working end to end. ArgoCD is
installed and its sync engine verified against a public repo; the `echo`
manifests apply cleanly and serve traffic.

The GitOps loop is verified end to end: a commit pushed to GitHub changed the
running replica count ~12 seconds later, with no `kubectl`. CI is green across
all eight jobs on GitHub Actions.

One gap remains, stated plainly: **the expensive Terraform tier has never been
applied.** The cheap tier has — applied to a real account, verified with the
smoke checks against live S3 and SNS/SQS, then destroyed. EKS and RDS are
plan-only, so nothing is proven against a live cluster or database.

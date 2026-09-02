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
  PASS  relay           684ms  evt_36fe9a6f159b2789ae59a8bf0d2bf896 delivered to /hooks/ok, dead-lettered http://sink:8081/hooks/flaky after 3 attempts; 4 attempts persisted

all components healthy
```

## Project status

GitHub Issues and milestones hold the current backlog. Open work records its
outcome, trigger, decision owner, governing anchors, dependency relation, and
checked evidence in the issue body.

| Milestone | State | Purpose |
|---|---|---|
| [M0: clear the runway](https://github.com/lilabrooks/my-local-platform/milestone/1) | Closed | Fix the smoke path, create relay topics, and settle the Kafka client. |
| [M1: the relay delivers](https://github.com/lilabrooks/my-local-platform/milestone/2) | Closed | Deliver one accepted event and dead-letter a refusing subscriber. |
| [M2: KEDA scaling on lag](https://github.com/lilabrooks/my-local-platform/milestone/3) | Closed | Scale relay-deliver on broker lag, drain the backlog, and scale down. |

M3 and M4 remain optional, unscheduled choices in the
[relay roadmap](docs/roadmap-relay.md). See the
[open issues](https://github.com/lilabrooks/my-local-platform/issues) for the
work queue and
[#84](https://github.com/lilabrooks/my-local-platform/issues/84) for the backlog
authority record.

## What is here

```text
local/          docker-compose stack, split into profiles
  bootstrap/    idempotent seed scripts for AWS resources and Kafka topics
  config/       OTel Collector, Prometheus, Tempo, Grafana provisioning
services/
  smoke/        Go service that writes to and reads back from every component
  echo/         small HTTP service, the workload ArgoCD deploys
  relay/        webhook delivery service: ingest to Kafka, deliver with retries
  sink/         subscriber relay delivers to, deliberately slow or failing
k8s/
  argocd/       ArgoCD install, scoped AppProjects, root Application
  apps/         one Application per workload (app-of-apps)
  manifests/    what those Applications point at
infra/terraform/
  bootstrap/    remote state backend, run once
  envs/dev/     the AWS environment, split cheap vs expensive
docs/adr/       why each choice was made, and what was verified
```

## The stack

| Component | Local | Real AWS counterpart |
|---|---|---|
| Object storage | floci S3 | S3 |
| Container-backed AWS | floci RDS/EKS (opt-in socket) | the real services |
| Pub/sub + queues | floci SNS/SQS | SNS + SQS with a DLQ |
| Email | floci SES | SES |
| Event streaming | Apache Kafka (KRaft) | MSK or self-managed |
| Message broker | RabbitMQ | Amazon MQ |
| Relational | Postgres 18 | RDS (`enable_rds`) |
| Kubernetes | minikube (`mlp` profile) | EKS (`enable_eks`) |
| Deployment | ArgoCD, app-of-apps | same manifests, ECR images |
| Telemetry | OTel → Prometheus + Tempo + Grafana | OTel → Datadog |

The Terraform currently implements S3, SNS/SQS, ECR, and optional SES, RDS,
and EKS. MSK is proposed for M4; Amazon MQ is a service counterpart, not a
resource this repository provisions.

`relay` and its sink are scraped by Prometheus directly and come with a
provisioned Grafana dashboard at
**<http://localhost:3000/d/relay-delivery>** — consumer lag against consumer
count, delivery latency, and the outcomes behind both. Details in the
[local runbook](docs/runbook-local.md#metrics-and-the-relay-dashboard).

## Design decisions

Nine choices shape this repository, each recorded with the evidence behind it:

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
- **[KEDA on lag, not an HPA on CPU](docs/adr/0007-keda-lag-autoscaling.md)** —
  measured: a consumer's CPU is *lower* with a 595-event backlog than with none,
  so an HPA would scale down exactly when consumers are needed.
- **[In-cluster Prometheus and Grafana](docs/adr/0008-in-cluster-observability-for-the-demo.md)** —
  the autoscaling demo's argument is a picture, so the thing that draws it has
  to live where the pods do. Amends ADR 0005, which had kept telemetry out of
  the cluster on purpose.
- **[Separate ArgoCD control and workload projects](docs/adr/0009-separate-argocd-control-and-workload-projects.md)**:
  the root may create child Applications, workloads stay inside `mlp`, and the
  built-in `default` project has no permissions.

The Kafka one covers `relay`, the first application — see
**[its goal](docs/goal-relay.md)** and **[roadmap](docs/roadmap-relay.md)**.
It is built through M2: `make up-apps` starts it, `make smoke` exercises the
whole pipeline end to end, and `make relay-demo` runs the autoscaling demo
against the cluster.

## The smoke service

`services/smoke` is the load-bearing piece of this repository. Every check
**writes something and reads it back**, then asserts the payload matches — a
check that merely opens a connection proves a port is listening, which is not
the same as the component working. It exits non-zero on failure, so it doubles
as a CI gate.

It is also the reference for how to talk to each component: the AWS SDK against
a custom endpoint, a Kafka produce-and-fetch at the returned partition and
offset, an AMQP round trip, `pgx`, and OTLP tracing that wraps every check in a
span.

## Kubernetes and GitOps

```bash
make k8s-up          # dedicated 'mlp' minikube profile
make echo-image      # build and load the workload image
make argocd-install  # ArgoCD + app-of-apps
make argocd-ui       # https://localhost:8081
```

ArgoCD pulls from a git URL. A private remote needs a read-only deploy key;
`make argocd-repo-creds` generates one, registers it, and repoints the live
Applications at the SSH URL. A public remote can use HTTPS without credentials.
Keep the tracked Application URLs and `REPO_URL` consistent with that choice.
`make k8s-apply-local` applies the same manifests directly, without git.
**[docs/runbook-k8s.md](docs/runbook-k8s.md)** covers the details.

The root Application uses `mlp-root`, which can create only Application objects
inside `argocd`. Child Applications use `mlp`, which is confined to namespace
`mlp`. ArgoCD's built-in `default` project is disabled.

## Real AWS

Requires an SSO session and the one-time remote-state bootstrap described in
[the cost guide](docs/costs.md#remote-state-comes-first):

```bash
make aws-login      # aws sso login
make aws-whoami
make aws-bootstrap  # once for a new account: versioned S3 state backend
make aws-init       # bind this checkout to the account-scoped backend
make aws-plan       # read-only; also runs aws-init as a prerequisite
make aws-up         # prompts before creating anything billable
make aws-cost       # month-to-date spend
make aws-down       # destroy through the already initialized backend
```

If the account already has the state bucket, skip `make aws-bootstrap`.
Override the defaults with Make variables, for example
`make aws-plan AWS_PROFILE_NAME=my-sso-profile AWS_REAL_REGION=us-west-2`.

The default environment is ten serverless resources that cost approximately
nothing idle. EKS and RDS are behind `enable_eks` and `enable_rds`, both
`false` by default. **[docs/costs.md](docs/costs.md)** has the arithmetic.

## Linting

```bash
make lint
```

Ten checks: Go, YAML, shell, Markdown, GitHub Actions workflows, every service
Dockerfile, Terraform formatting and lint rules, infrastructure security, and a
secret scan across git history.

Every linter is pinned, and a locally installed binary is used **only when it
reports the pinned version** — otherwise a pinned container runs instead, so
Docker alone is enough. Preferring whatever was on `PATH` is how `make lint` and
CI came to run different shellchecks and disagree on the same commit.

CI runs this script rather than its own copy of the ten checks, for the same
reason. A linter that can run neither way reports `SKIP` locally; under
`LINT_STRICT`, which CI sets, a skip is a failure — there, a gate that did not
run must not read as one that passed.

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

Docker, Go 1.27+, Terraform 1.10+, the AWS CLI v2, minikube, kubectl and Helm.

Helm is needed only by `make monitoring-install`, which renders
`kube-prometheus-stack` into the cluster so the relay demo has a panel to point
at — see [ADR 0008](docs/adr/0008-in-cluster-observability-for-the-demo.md).
Everything else in the cluster is plain manifests applied by kubectl or synced
by ArgoCD, and no CI job touches a cluster, so nothing else needs it.

Tested with **Helm v4.2.4**. 3.x is untested rather than known-broken — the
chart declares `apiVersion: v2`, which both majors read.

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
running replica count ~12 seconds later, with no `kubectl`. The last main run
that received GitHub-hosted runners passed all thirteen jobs.

**`relay` is built through M2 and its demo runs.** `make relay-demo` drives six
steps in 190 seconds against minikube: an event delivered, the subscriber slowed,
KEDA scaling the consumer group from one pod to twelve on lag, the backlog
drained, a failing subscriber dead-lettered while a healthy one is unaffected,
and the whole window replayed from the log. The measurements behind that are in
[ADR 0007](docs/adr/0007-keda-lag-autoscaling.md), including why an HPA on CPU
cannot work here.

One gap remains, stated plainly: **the expensive Terraform tier has never been
applied.** The cheap tier has — applied to a real account, verified with the
smoke checks against live S3 and SNS/SQS, then destroyed. EKS and RDS are
plan-only, so nothing is proven against a live cluster or database.

## License

Licensed under the [Apache License 2.0](LICENSE).

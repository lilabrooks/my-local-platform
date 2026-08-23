# my-local-platform

A distributed-systems playground that runs on a laptop for free, and on real
AWS when it needs to.

Kafka, RabbitMQ, Postgres, S3, SNS, SQS, SES, OpenTelemetry, Prometheus, Tempo
and Grafana come up with one command and no cloud account. EKS and RDS are one
Terraform flag away when emulation is not enough.

```bash
cp .env.example .env
make up
make smoke
```

```
smoke check  aws=http://localhost:4566 region=us-east-1 kafka=localhost:9092

  PASS  s3               32ms  s3://mlp-artifacts/smoke/1787525730971651000.txt round trip
  PASS  sns->sqs         17ms  fanout delivered smoke-1787525731015236000
  PASS  ses              24ms  sent message b4be37fb-9614-4a34-8c5e-02aa26220736
  PASS  kafka         10091ms  mlp.events partition 0 offset 1
  PASS  rabbitmq          7ms  queue mlp.smoke round trip
  PASS  postgres         11ms  row 2 on postgres 17.11

all components healthy
```

## What is here

```
local/          docker-compose stack, profiled by memory footprint
  bootstrap/    idempotent seed scripts for AWS resources and Kafka topics
  config/       OTel Collector, Prometheus, Tempo, Grafana provisioning
services/smoke/ Go service that writes to and reads back from every component
infra/terraform/
  bootstrap/    remote state backend, run once
  envs/dev/     the AWS environment, split cheap vs expensive
docs/adr/       why each choice was made, and what was verified
```

## The stack

| Component | Local | Real AWS |
|---|---|---|
| Object storage | floci S3 | S3 |
| Pub/sub + queues | floci SNS/SQS | SNS + SQS with a DLQ |
| Email | floci SES | SES |
| Event streaming | Apache Kafka (KRaft) | MSK, or self-managed |
| Message broker | RabbitMQ | Amazon MQ |
| Relational | Postgres 17 | RDS (`enable_rds`) |
| Kubernetes | minikube | EKS (`enable_eks`) |
| Telemetry | OTel → Prometheus + Tempo + Grafana | OTel → Datadog |

## Design decisions

Four choices shape this repository, each recorded with the evidence behind it:

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

## The smoke service

`services/smoke` is the load-bearing piece of this repository. Every check
**writes something and reads it back**, then asserts the payload matches — a
check that merely opens a connection proves a port is listening, which is not
the same as the component working. It exits non-zero on failure, so it doubles
as a CI gate.

It is also the reference for how to talk to each component: the AWS SDK against
a custom endpoint, a Kafka producer and consumer group, an AMQP round trip,
`pgx`, and OTLP tracing that wraps every check in a span.

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

## Requirements

Docker, Go 1.27+, Terraform 1.9+, the AWS CLI v2. `make help` lists every
target; **[docs/runbook-local.md](docs/runbook-local.md)** covers ports,
credentials and troubleshooting.

## Status

The local stack and the smoke checks are verified working end to end. The
Terraform is validated and plans cleanly against a real account — 65 resources
with both expensive flags on, 10 with the defaults — but has **not been
applied**, so nothing here is proven against live EKS or RDS yet.

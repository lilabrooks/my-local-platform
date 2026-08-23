# Local runbook

## First run

```bash
cp .env.example .env
make up
make smoke
```

`make up` starts every profile and seeds resources. Expect the first run to
take a few minutes while images download.

## Profiles

The full stack idles at roughly 1 GB across all nine containers, so it fits a
default Docker Desktop allocation comfortably. Profiles are about startup time
and signal, not memory -- bring up only what you need:

| Command | Starts |
|---|---|
| `make up-core` | floci (AWS surface), Postgres |
| `make up-messaging` | Kafka, Kafka UI, RabbitMQ |
| `make up-obs` | OTel Collector, Prometheus, Tempo, Grafana |
| `make up` | everything |

`make urls` prints every endpoint.

## Endpoints

| Service | Address | Credentials |
|---|---|---|
| floci (AWS) | http://localhost:4566 | any; `test`/`test` by convention |
| Kafka | localhost:9092 | none |
| Kafka UI | http://localhost:8080 | none |
| RabbitMQ | localhost:5672 | guest/guest |
| RabbitMQ UI | http://localhost:15672 | guest/guest |
| Postgres | localhost:5432 | platform/platform |
| OTLP gRPC | localhost:4317 | none |
| Prometheus | http://localhost:9090 | none |
| Tempo | http://localhost:3200 | none |
| Grafana | http://localhost:3000 | anonymous, admin role |

## Talking to the local AWS surface

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
unset AWS_PROFILE          # otherwise a real SSO profile can shadow these

aws s3 ls
aws sns list-topics
```

`unset AWS_PROFILE` matters. With a profile set, the CLI tries to resolve it
and fails with `The config profile could not be found` — which looks like a
floci problem and is not.

## Troubleshooting

**`make smoke` fails on s3 or sns->sqs.** The resources are missing. Run
`make seed`. This happens after `docker compose down -v`, which deletes the
floci volume.

**Kafka check is slow (~10s).** Expected. The consumer uses a fresh group id
each run and waits out the initial group-join.

**Grafana shows no traces.** Tempo's ingester takes ~15-20 seconds after start
before it reports ready. Check `curl http://localhost:3200/ready`.

**`otel-collector` container exits immediately.** Almost always a config error;
`docker logs mlp-otel-collector` states it plainly. If you enabled the Datadog
config without `DD_API_KEY`, it exits with
`exporters::datadog: api.key is not set`.

**Ports already in use.** This stack claims 3000, 3200, 4317, 4318, 4566, 5432,
5672, 8080, 8889, 9090, 9092 and 15672.

## Resetting

```bash
make down     # stop, keep data
make clean    # stop and delete all volumes -- re-seed afterwards
```

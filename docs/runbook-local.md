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

Profiles exist because memory is a real constraint. An earlier note here
claimed the stack idles at ~1 GB and that memory was not the binding factor --
that was measured 46 seconds after startup and was wrong. Under sustained use
the JVM services grow substantially:

| Profile | Sustained | Contains |
|---|---|---|
| `core` | ~140 MB | floci, postgres |
| `messaging` | ~660 MB | kafka, rabbitmq |
| `tools` | ~285 MB | kafka-ui (needs `messaging`) |
| `obs` | ~490 MB | collector, prometheus, tempo, grafana |
| **all** | **~1.6 GB** | everything above |

A local minikube adds **~1.8 GB** on top, making it the single largest
consumer. `make k8s-down` when you are not doing GitOps work.

`make mem` prints current usage. Bring up only what you need:

| Command | Starts |
|---|---|
| `make up-core` | floci (AWS surface), Postgres |
| `make up-messaging` | Kafka, RabbitMQ |
| `make up-tools` | adds Kafka UI |
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
| OTLP HTTP | localhost:4318 | none |
| Collector metrics | localhost:8889 | none |
| Prometheus | http://localhost:9090 | none |
| Tempo | http://localhost:3200 | none |
| Grafana | http://localhost:3000 | anonymous, admin role |
| relay dashboard | http://localhost:3000/d/relay-delivery | anonymous, admin role |

## Metrics and the relay dashboard

relay and the sink expose Prometheus text on `/metrics`, and Prometheus scrapes
them directly rather than through the OTel collector. The collector re-exports
what arrives over OTLP, and putting a consumer-lag gauge through that hop adds
a place for it to be lost between the thing that measures it and the panel that
plots it.

```bash
curl -s localhost:8082/metrics | grep relay_consumer_group_lag_total
curl -s localhost:8083/metrics | grep relay_deliveries_total
curl -s localhost:8084/metrics | grep sink_latency_ms
```

**Consumer lag is published by ingest, not by the consumers.** A deliver pod
knows only the partitions it holds, and the M2 demo moves that group between one
and twelve members, so per-pod lag series appear and vanish and their sum is
least trustworthy exactly while someone is watching it. `relay-ingest` reads the
group's committed offsets straight from the broker instead, which is also where
KEDA reads them — so the dashboard and the scaler agree by construction.

`relay_lag_refreshed_timestamp_seconds` is how you tell a drained topic from a
lag nobody could measure. The gauges hold their last value when a poll fails,
so the dashboard carries an **Age of the lag measurement** panel; anything past
about 20s means the lag panels are stale rather than calm. The 20s is a 5s poll
interval (`RELAY_LAG_INTERVAL`) plus up to a 15s Prometheus scrape interval.

Prometheus finds relay through DNS service discovery rather than a static
target, so `docker compose --profile apps up -d --scale relay-deliver=3` is
scraped as three instances and `count(relay_build_info{role="deliver"})` is a
real consumer count. With only `make up-obs` running, the `apps` names do not
resolve and those jobs simply have no targets — expected, not a fault.

Provisioning lives in `local/config/grafana/provisioning/`. Dashboards are files
in this repository and `allowUiUpdates` is false, so an accidental save in the
browser is discarded on restart; edit `dashboards/relay.json` instead. The
datasource uids are pinned (`prometheus`, `tempo`) because a provisioned
dashboard has to name one, and an unpinned uid is generated per install — the
panels would work on the machine they were authored on and come up "Datasource
not found" everywhere else.

## floci and the Docker socket

The base compose file does **not** give floci the Docker socket. That mount
grants a container effective root on the host, and everything the smoke checks
use — S3, SNS, SQS, SES, Secrets Manager — works without it.

floci's *container-backed* services do need it, because they start real sibling
containers: **RDS, EKS, Lambda, MSK, ElastiCache, OpenSearch**. Without the
socket they fail with a docker-java `UnixSocket` error and no container appears.

Opt in only while you need them:

```bash
make up-core-containers
```

## Other make targets

`make help` lists everything. Beyond the ones above: `make fmt`, `make vet`,
`make tidy` for Go housekeeping, `make ps` and `make logs SVC=<name>` for
inspecting the stack, and `make mem` for current memory use.

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

## Running the smoke checks against real AWS

The same binary works against the real account. Only environment changes:

```bash
cd infra/terraform/envs/dev
terraform apply                       # creates the cheap tier, ~$0
BUCKET=$(terraform output -raw bucket)

cd ../../../services/smoke
AWS_PROFILE=aws-public-change-feed \
AWS_DEFAULT_REGION=us-east-1 \
MLP_USE_REAL_AWS=1 \
MLP_BUCKET="$BUCKET" \
MLP_TOPIC=mlp-dev-events \
MLP_QUEUE=mlp-dev-events \
  go run ./cmd/smoke
```

`MLP_USE_REAL_AWS=1` is the **only** way to reach a live account. An empty or
unset `AWS_ENDPOINT_URL` still means "local", so a stray
`export AWS_ENDPOINT_URL=` cannot silently redirect these checks at production.

Expect S3 and SNS-to-SQS to take roughly a second each rather than tens of
milliseconds. SES is skipped on purpose — a smoke check should not send real
email. Run `terraform destroy` when finished.

## Troubleshooting

**`make smoke` fails on s3 or sns->sqs.** The resources are missing. Run
`make seed`. This happens after `docker compose down -v`, which deletes the
floci volume.

**Kafka check is slow (~10s).** Expected. The consumer uses a fresh group id
each run and waits out the initial group-join.

**Grafana shows no traces.** Three separate causes, in order of likelihood:

1. **Tempo 3.x search needs an explicit time range.** A bare
   `/api/search?tags=...` returns `{"traces":[]}` even when traces exist, which
   looks like data loss and is not. Pass `start` and `end` as unix seconds:

   ```bash
   NOW=$(date +%s)
   curl -sS "http://localhost:3200/api/search?tags=service.name%3Dsmoke&start=$((NOW-600))&end=${NOW}"
   ```

2. **The collector's connection to Tempo goes stale if Tempo restarts.** The
   collector logs `no children to pick from` and retries forever.
   `docker compose -f local/docker-compose.yml --profile obs restart otel-collector`.

3. Tempo takes ~5-20 seconds after start to report ready. Check
   `curl http://localhost:3200/ready`.

**`otel-collector` container exits immediately.** Almost always a config error;
`docker logs mlp-otel-collector` states it plainly. If you enabled the Datadog
config without `DD_API_KEY`, it exits with
`exporters::datadog: api.key is not set`.

**Ports already in use.** This stack claims 3000, 3200, 4317, 4318, 4566, 5432,
5672, 8080, 8889, 9090, 9092 and 15672.

### Grafana exits with "Datasource provisioning error: data source not found"

A `grafana-data` volume created before the datasource uids were pinned holds
`Prometheus` under a generated uid. Provisioning matches by uid, not by name, so
it finds nothing to update and **fails the whole provisioning module** — Grafana
then exits rather than starting without datasources, and the container restart
loops.

`datasources.yml` deletes both datasources by name before recreating them, which
makes it idempotent against either kind of volume. If a stale volume still gets
in the way:

```bash
docker compose -f local/docker-compose.yml rm -sf grafana && docker volume rm mlp_grafana-data
```

Nothing is lost. Every dashboard and datasource here is provisioned from files.

## Upgrading Postgres past 17

Postgres 18 changed where the official image expects its volume. It now writes
to a major-version subdirectory (`/var/lib/postgresql/18/docker`) so that
`pg_upgrade --link` can run without crossing a mount boundary, and it refuses
to start if the old `/var/lib/postgresql/data` path is mounted:

```text
there appears to be PostgreSQL data in: /var/lib/postgresql/data
(unused mount/volume) ... container exits 1
```

The compose file mounts the new path, and the volume was renamed `pg-data` ->
`postgres-data` at the same time. That rename is what keeps `git pull &&
make up` working: the old volume holds a 17-format cluster at its root, which
18 cannot read at the new mount point, so reusing the name would have turned a
routine pull into a hard failure.

Nothing is migrated — this database holds disposable smoke-check rows. The old
volume is orphaned and safe to delete once you are happy:

```bash
docker volume rm mlp_pg-data
```

## Resetting

```bash
make down     # stop, keep data
make clean    # stop and delete all volumes -- re-seed afterwards
```

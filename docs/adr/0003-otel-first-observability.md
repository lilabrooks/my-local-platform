# 3. OpenTelemetry-first, with Datadog as one exporter

Date: 2026-08-23
Status: Accepted

## Context

Datadog is on the list of things to learn, and it is what many employers
actually run. It is also a paid product: a repository whose telemetry only
works with a valid Datadog API key stops working when a trial lapses, and never
works at all for anyone else who clones it.

## Decision

Services emit OpenTelemetry over OTLP and name no vendor. An OTel Collector
decides where telemetry lands.

Two collector configurations exist:

- `local/config/otel/config.yaml` (default) -- Prometheus, Tempo, debug.
- `local/config/otel/config.datadog.yaml` -- the same, plus Datadog.

Selected at runtime:

```bash
OTEL_COLLECTOR_CONFIG=config.datadog.yaml make up-obs
```

Application code is byte-identical either way.

## Consequences

The repository is fully functional with no accounts, and Datadog becomes a
one-variable change rather than a rewrite. The trade is a layer of indirection
and one more container, plus the fact that vendor-specific Datadog features
(APM-native profiling, some integrations) are not exercised by the OTLP path.

The two config files duplicate their receiver and processor blocks. That
duplication is deliberate: the alternative is a single file where the Datadog
exporter is present but unused, which does not work -- see below.

## Verification

The naive design -- one config file with a `datadog` exporter defined but left
out of the pipelines -- **fails**. The collector validates every *defined*
exporter at startup, not only those a pipeline references, so an empty
`DD_API_KEY` crashes it:

```text
Error: invalid configuration: exporters::datadog: api.key is not set
```

That is why Datadog lives in a separate file rather than behind a comment.

End-to-end, with the default config: `make smoke` produces spans that reach
Tempo, confirmed by querying it directly rather than by trusting that the SDK
initialised.

```bash
NOW=$(date +%s)
curl -sS "http://localhost:3200/api/search?tags=service.name%3Dsmoke&start=$((NOW-600))&end=${NOW}"
# -> traces with root span "smoke.run"
```

The relay path was checked on 2026-09-03 with relay already running and the
observability profile restarted afterward, using the same command as CI:

```bash
docker compose --env-file .env.example -f local/docker-compose.yml \
  --profile obs up -d --wait --wait-timeout 300
make smoke-traces
```

The observability containers had been stopped after `make up`, so this sequence
exercised relay's exporter reconnecting after the collector became available.
The trace-id query in `docs/runbook-local.md` then confirmed the result. Event
`evt_346fc744d63aa3ac23ea11d068f99232` produced one Tempo trace containing
`relay.ingest`, `kafka.produce`, `relay.consume`, and four
`relay.webhook.attempt` spans — one successful request to the healthy
subscriber, and three to the failing one before it was dead-lettered — all under
trace `4ca8d2381dc5be5e0d6660db478f11f0`.

The M3 whole-application proof repeated the assertion on 2026-09-05. Event
`evt_2f90004ddafd451a18789e98a64f794e` produced the same ingest, produce, and
consume path plus 4 attempt spans under trace
`2b74c971058f50dc7219eddfce5f5cf9`. `make smoke-traces` exited 0 after matching
every attempt span to a persisted attempt row.

`make smoke-traces` is what asserts this, rather than `make smoke`. Tempo is in
the `obs` compose profile and relay is in `apps`, so the plain smoke run — and
CI's first smoke step, which excludes `obs` deliberately to prove tracing is
best effort — would otherwise fail for a missing dependency rather than a
missing trace. The assertion is a count, not a name check: one `relay.webhook.attempt`
span per row in the event's persisted attempt history, so per-attempt spans
collapsing into one fails the check.

**What a span may carry is a boundary, not a blanket ban on caller input.**
Spans deliberately do carry `relay.tenant.id`, `relay.event.type` and the
caller's incoming `tracestate` — all caller-supplied, all identifiers, and all
useless to omit: a trace you cannot filter by tenant is a trace nobody queries
twice. What they must never carry is the event payload, a signing secret, a
subscriber URL, or an error message.

The last two are the ones this work got wrong. `http.Client` returns a
`*url.Error` whose text embeds the subscriber URL with its path and query
string, and an idempotency conflict names the caller's key; both reached spans
as `exception.message` until relay stopped calling `span.RecordError` and began
recording an `error.type` classification with a fixed status description
instead. The smoke service broke the same rule from the other side, putting a
check's success detail on a `check.detail` attribute — a 2026-09-03 trace
contained `dead-lettered http://sink:8081/hooks/flaky` there. Instrumenting
relay and forgetting the service that watches relay moves a leak rather than
closing it.

Both boundaries are enforced by recorder tests rather than by comment:
`telemetry.TestRecordErrorKeepsErrorMessagesOffTheSpan` and
`checks.TestInstrumentKeepsCheckStringsOffSpans`. Neither can prove the absence
of a leak in a span they do not construct, which is how the smoke one survived
the round that fixed relay's.

Two things about Tempo 3.x that look like data loss and are not:

- **Search needs an explicit time range.** Without `start`/`end` it returns
  `{"traces":[]}` even when `tempo_distributor_spans_received_total` proves
  spans arrived. The 2.x form of this command, with no range, silently returns
  nothing.
- **The collector's gRPC connection goes stale if Tempo restarts**, logging
  `no children to pick from` and retrying forever. Restart the collector.

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

```
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

```
Error: invalid configuration: exporters::datadog: api.key is not set
```

That is why Datadog lives in a separate file rather than behind a comment.

End-to-end, with the default config: `make smoke` produces spans that reach
Tempo, confirmed by querying it directly rather than by trusting that the SDK
initialised.

```
curl -sS "http://localhost:3200/api/search?tags=service.name%3Dsmoke&limit=5"
# -> 2 traces, root span "smoke.run"
```

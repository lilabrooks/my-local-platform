#!/usr/bin/env bash
# Create relay's schema and seed local subscriptions. Idempotent.
set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-mlp-postgres}"
PG_USER="${POSTGRES_USER:-platform}"
PG_DB="${POSTGRES_DB:-platform}"

# The signing secret has to match what the sink verifies with, or every
# delivery is rejected 401. Both this and local/docker-compose.yml default to
# the same value so `make up && make seed` works with no further setup -- the
# same reasoning as the postgres and rabbitmq credentials already in compose.
#
# It is a local development value, not a secret. Override it here and in the
# sink's environment together to use something else.
RELAY_SIGNING_SECRET="${RELAY_SIGNING_SECRET:-mlp-local-dev-signing-key}"

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

say "waiting for postgres in $PG_CONTAINER"
for i in $(seq 1 40); do
  if docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then break; fi
  [ "$i" = 40 ] && { echo "postgres not ready" >&2; exit 1; }
  sleep 2
done

say "relay: schema and subscriptions"
# The secret arrives as a container environment variable and is pulled in with
# psql's \getenv, so it never appears in process arguments the way -v would.
docker exec -i -e RELAY_SIGNING_SECRET="$RELAY_SIGNING_SECRET" \
  "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -q -U "$PG_USER" -d "$PG_DB" <<'SQL'
\getenv secret RELAY_SIGNING_SECRET
CREATE TABLE IF NOT EXISTS relay_subscriptions (
    id             bigserial   PRIMARY KEY,
    tenant_id      text        NOT NULL,
    url            text        NOT NULL,
    signing_secret text        NOT NULL,
    active         boolean     NOT NULL DEFAULT true,
    created_at     timestamptz NOT NULL DEFAULT now(),

    -- One subscription per endpoint per tenant. This is also what makes the
    -- seed below idempotent.
    CONSTRAINT relay_subscriptions_tenant_url UNIQUE (tenant_id, url)
);

-- The delivery consumer's only query is "active subscriptions for this tenant".
CREATE INDEX IF NOT EXISTS relay_subscriptions_active_tenant
    ON relay_subscriptions (tenant_id) WHERE active;

-- Ingest creates this row before publishing to Kafka. Delivery attempts can
-- therefore reference the event even when a consumer fetches it immediately.
-- A row is retained when Kafka publication reports an error because that error
-- can be ambiguous: the broker may still have accepted the record.
CREATE TABLE IF NOT EXISTS relay_events (
    id                     text        PRIMARY KEY,
    tenant_id              text        NOT NULL,
    event_type             text        NOT NULL,
    data                   jsonb       NOT NULL,
    data_raw               text        NOT NULL,
    idempotency_key        text,
    idempotency_claimed_at timestamptz,
    occurred_at            timestamptz NOT NULL,
    accepted_at            timestamptz NOT NULL DEFAULT now(),
    published_at           timestamptz
);

-- Existing jsonb values have already lost their original lexical form. Keep
-- that representation as the best available raw value; new writes preserve the
-- request bytes separately while data remains jsonb for semantic comparison.
ALTER TABLE relay_events
    ADD COLUMN IF NOT EXISTS data_raw text;
UPDATE relay_events SET data_raw = data::text WHERE data_raw IS NULL;
ALTER TABLE relay_events ALTER COLUMN data_raw SET NOT NULL;

-- Rows from before this migration may contain duplicate or ambiguous keys
-- because idempotency_key used to be inert metadata. They remain unclaimed so
-- the migration neither deletes that history nor treats an old row as a new
-- tenant-facing guarantee.
ALTER TABLE relay_events
    ADD COLUMN IF NOT EXISTS idempotency_claimed_at timestamptz;
ALTER TABLE relay_events
    ADD COLUMN IF NOT EXISTS published_at timestamptz;

CREATE INDEX IF NOT EXISTS relay_events_tenant_accepted
    ON relay_events (tenant_id, accepted_at DESC);

-- Remove the predicate used by the unpublished development draft, if it was
-- applied locally. It included historical keys in the new uniqueness contract.
DROP INDEX IF EXISTS relay_events_tenant_idempotency;

-- Unclaimed historical keys and blank new keys are intentionally unlimited.
-- A claimed key names one request per tenant and is the row lock that
-- serializes concurrent submissions.
CREATE UNIQUE INDEX IF NOT EXISTS relay_events_tenant_idempotency_claimed
    ON relay_events (tenant_id, idempotency_key)
    WHERE idempotency_claimed_at IS NOT NULL;

-- A failed attempt-history write leaves the Kafka offset uncommitted. The
-- subscriber may therefore see a duplicate on redelivery, which is relay's
-- at-least-once contract, while an unrecorded delivery cannot be committed.
CREATE TABLE IF NOT EXISTS relay_delivery_attempts (
    id               bigserial   PRIMARY KEY,
    event_id         text        NOT NULL REFERENCES relay_events (id) ON DELETE CASCADE,
    subscription_id  bigint      NOT NULL,
    subscription_url text        NOT NULL,
    attempt_number   integer     NOT NULL CHECK (attempt_number > 0),
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz NOT NULL,
    http_status      integer,
    transport_error  text,
    outcome          text        NOT NULL,

    CONSTRAINT relay_delivery_attempts_time_order
        CHECK (finished_at >= started_at),
    CONSTRAINT relay_delivery_attempts_result
        CHECK ((http_status IS NULL) <> (transport_error IS NULL)),
    CONSTRAINT relay_delivery_attempts_http_status
        CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    CONSTRAINT relay_delivery_attempts_outcome
        CHECK (outcome IN ('delivered', 'retrying', 'exhausted', 'interrupted')),
    CONSTRAINT relay_delivery_attempts_sequence
        UNIQUE (event_id, subscription_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS relay_delivery_attempts_event_order
    ON relay_delivery_attempts (event_id, started_at, id);

-- An existing attempt proves that a consumer read the event from Kafka. Rows
-- without that evidence stay unknown rather than being backfilled as published;
-- issue #87 deliberately retained rows after ambiguous producer failures.
UPDATE relay_events AS event
SET published_at = event.accepted_at
WHERE event.published_at IS NULL
  AND EXISTS (
      SELECT 1
      FROM relay_delivery_attempts AS attempt
      WHERE attempt.event_id = event.id
  );

-- Two subscribers for one tenant, so the partial-failure path is exercisable:
-- one healthy, one pointed at a sink configured to fail. A single subscriber
-- would let a bug that dead-letters the whole event pass unnoticed.
INSERT INTO relay_subscriptions (tenant_id, url, signing_secret) VALUES
    ('acme',   'http://sink:8081/hooks/ok',    :'secret'),
    ('acme',   'http://sink:8081/hooks/flaky', :'secret'),
    ('globex', 'http://sink:8081/hooks/ok',    :'secret')
ON CONFLICT (tenant_id, url) DO UPDATE
SET signing_secret = EXCLUDED.signing_secret;

-- Tenants for the autoscaling demo, one healthy subscriber each.
--
-- Sixteen of them because the partition key is the tenant id, so a single
-- tenant's events all land on ONE partition and exactly one consumer can ever
-- work on them. Scaling to twelve pods against one busy partition would add
-- eleven idle pods and drain nothing -- the demo would show KEDA reacting and
-- nothing getting faster.
--
-- Sixteen tenants over twelve partitions leaves few partitions empty once the
-- hash spreads them, which is what gives added pods something to pick up.
--
-- No flaky endpoint here on purpose: dead-lettering is demonstrated with acme,
-- and mixing it into the scaling run would make the lag curve harder to read.
INSERT INTO relay_subscriptions (tenant_id, url, signing_secret)
SELECT 'demo-' || to_char(n, 'FM00'), 'http://sink:8081/hooks/ok', :'secret'
FROM generate_series(1, 16) AS n
ON CONFLICT (tenant_id, url) DO UPDATE
SET signing_secret = EXCLUDED.signing_secret;
SQL

say "relay database ready"
docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
  'SELECT tenant_id, url, active FROM relay_subscriptions ORDER BY tenant_id, url'

say "subscription signing secret configured (value not printed)"

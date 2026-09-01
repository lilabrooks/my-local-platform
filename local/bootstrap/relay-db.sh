#!/usr/bin/env bash
# Create relay's schema and seed local subscriptions. Idempotent.
#
# Nothing writes this table at runtime -- relay's delivery consumer only reads
# it. Subscriptions are configuration in M1, not a managed resource.
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

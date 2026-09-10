#!/usr/bin/env bash
# Create relay's schema and seed local subscriptions. Idempotent.
set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-mlp-postgres}"
PG_USER="${POSTGRES_USER:-platform}"
PG_DB="${POSTGRES_DB:-platform}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA="${SCRIPT_DIR}/../../services/relay/internal/bootstrap/schema.sql"

# The signing secret has to match what the sink verifies with, or every
# delivery is rejected 401. Both this and local/docker-compose.yml default to
# the same value so `make up && make seed` works with no further setup.
# It is a local development value, not a live credential.
RELAY_SIGNING_SECRET="${RELAY_SIGNING_SECRET:-mlp-local-dev-signing-key}"

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

say "waiting for postgres in $PG_CONTAINER"
for i in $(seq 1 40); do
  if docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then break; fi
  [ "$i" = 40 ] && { echo "postgres not ready" >&2; exit 1; }
  sleep 2
done

say "relay: schema and subscriptions"
# psql pulls the value from its environment, then places it in a transaction-
# local setting consumed by the shared SQL. The value never enters argv or the
# schema text.
{
  printf '%s\n' '\getenv secret RELAY_SIGNING_SECRET'
  printf '%s\n' 'BEGIN;' '\o /dev/null'
  printf '%s\n' "SELECT set_config('mlp.signing_secret', :'secret', true);" '\o'
  command cat "$SCHEMA"
  printf '%s\n' 'COMMIT;'
} | docker exec -i -e RELAY_SIGNING_SECRET="$RELAY_SIGNING_SECRET" \
  "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -q -U "$PG_USER" -d "$PG_DB"

say "relay database ready"
docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c \
  'SELECT tenant_id, url, active FROM relay_subscriptions ORDER BY tenant_id, url'

say "subscription signing secret configured (value not printed)"

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

CREATE INDEX IF NOT EXISTS relay_subscriptions_active_tenant
    ON relay_subscriptions (tenant_id) WHERE active;

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

ALTER TABLE relay_events
    ADD COLUMN IF NOT EXISTS data_raw text;
UPDATE relay_events SET data_raw = data::text WHERE data_raw IS NULL;
ALTER TABLE relay_events ALTER COLUMN data_raw SET NOT NULL;

ALTER TABLE relay_events
    ADD COLUMN IF NOT EXISTS idempotency_claimed_at timestamptz;
ALTER TABLE relay_events
    ADD COLUMN IF NOT EXISTS published_at timestamptz;

CREATE INDEX IF NOT EXISTS relay_events_tenant_accepted
    ON relay_events (tenant_id, accepted_at DESC);

DROP INDEX IF EXISTS relay_events_tenant_idempotency;

CREATE UNIQUE INDEX IF NOT EXISTS relay_events_tenant_idempotency_claimed
    ON relay_events (tenant_id, idempotency_key)
    WHERE idempotency_claimed_at IS NOT NULL;

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

UPDATE relay_events AS event
SET published_at = event.accepted_at
WHERE event.published_at IS NULL
  AND EXISTS (
      SELECT 1
      FROM relay_delivery_attempts AS attempt
      WHERE attempt.event_id = event.id
  );

INSERT INTO relay_subscriptions (tenant_id, url, signing_secret) VALUES
    ('acme',   'http://sink:8081/hooks/ok',    current_setting('mlp.signing_secret')),
    ('acme',   'http://sink:8081/hooks/flaky', current_setting('mlp.signing_secret')),
    ('globex', 'http://sink:8081/hooks/ok',    current_setting('mlp.signing_secret'))
ON CONFLICT (tenant_id, url) DO UPDATE
SET signing_secret = EXCLUDED.signing_secret;

INSERT INTO relay_subscriptions (tenant_id, url, signing_secret)
SELECT 'demo-' || to_char(n, 'FM00'),
       'http://sink:8081/hooks/ok',
       current_setting('mlp.signing_secret')
FROM generate_series(1, 16) AS n
ON CONFLICT (tenant_id, url) DO UPDATE
SET signing_secret = EXCLUDED.signing_secret;

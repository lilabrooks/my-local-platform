// Package history persists accepted relay events and subscriber delivery attempts.
package history

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
)

const (
	OutcomeDelivered   = "delivered"
	OutcomeRetrying    = "retrying"
	OutcomeExhausted   = "exhausted"
	OutcomeInterrupted = "interrupted"
)

var (
	ErrNoPool              = errors.New("history: no database pool")
	ErrEventNotFound       = errors.New("history: event not found")
	ErrIdempotencyConflict = errors.New("history: idempotency key conflicts with another request")
	ErrPublishFailed       = errors.New("history: event publication failed")
)

// Acceptance is the durable event chosen for an ingest request. Deduplicated
// means an earlier request already published it, so the caller must return its
// id without writing another Kafka record.
type Acceptance struct {
	Record       event.Record
	Deduplicated bool
}

// Publisher writes the chosen event to relay's delivery log.
type Publisher func(context.Context, event.Record) error

// Attempt is one completed HTTP request to one subscription. AttemptNumber is
// assigned by Store.RecordAttempt across every processing of an event, so a
// Kafka redelivery appends history instead of overwriting the first run.
type Attempt struct {
	SubscriptionID  int64     `json:"subscription_id"`
	SubscriptionURL string    `json:"subscription_url"`
	AttemptNumber   int       `json:"attempt_number"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	HTTPStatus      *int      `json:"http_status,omitempty"`
	TransportError  string    `json:"transport_error,omitempty"`
	Outcome         string    `json:"outcome"`
}

// Store owns relay's durable event and attempt history. The caller owns the
// pool lifetime.
type Store struct {
	pool            *pgxpool.Pool
	acceptanceSlots chan struct{}
}

func New(pool *pgxpool.Pool) *Store {
	if pool == nil {
		return &Store{}
	}
	// A Kafka write holds one transaction connection. Leave one configured
	// pool connection available for attempt-history reads and other operations
	// while the broker is slow or unavailable.
	slots := int(pool.Config().MaxConns) - 1
	if slots < 1 {
		slots = 1
	}
	return &Store{pool: pool, acceptanceSlots: make(chan struct{}, slots)}
}

// AcceptEvent persists an event before publishing it, then serializes requests
// sharing a tenant and idempotency key on that row. The row is committed before
// publish so a fast Kafka consumer can attach its attempt history immediately.
// published_at changes only after Publisher returns nil, which keeps an
// ambiguous or failed publish from becoming a successful deduplication result.
func (s *Store) AcceptEvent(ctx context.Context, rec event.Record, publish Publisher) (Acceptance, error) {
	if s == nil || s.pool == nil {
		return Acceptance{}, ErrNoPool
	}
	if publish == nil {
		return Acceptance{}, errors.New("history: no event publisher")
	}
	if err := s.acquireAcceptanceSlot(ctx); err != nil {
		return Acceptance{}, err
	}
	defer func() { <-s.acceptanceSlots }()
	if strings.TrimSpace(rec.IdempotencyKey) == "" {
		rec.IdempotencyKey = ""
	}

	tag, err := s.pool.Exec(ctx, `
		INSERT INTO relay_events
			(id, tenant_id, event_type, data, data_raw, idempotency_key,
			 idempotency_claimed_at, occurred_at, published_at)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''),
			CASE WHEN NULLIF($6, '') IS NULL THEN NULL ELSE now() END, $7, NULL)
		ON CONFLICT DO NOTHING`,
		rec.ID, rec.TenantID, rec.Type, rec.Data, string(rec.Data),
		rec.IdempotencyKey, rec.OccurredAt)
	if err != nil {
		return Acceptance{}, fmt.Errorf("reserve event %s: %w", rec.ID, err)
	}
	if rec.IdempotencyKey == "" && tag.RowsAffected() != 1 {
		return Acceptance{}, fmt.Errorf("reserve event %s: generated id already exists", rec.ID)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Acceptance{}, fmt.Errorf("begin event acceptance transaction: %w", err)
	}
	defer rollback(tx)

	accepted, published, matches, err := lockAcceptedEvent(ctx, tx, rec)
	if err != nil {
		return Acceptance{}, err
	}
	if !matches {
		if rec.IdempotencyKey != "" {
			return Acceptance{}, fmt.Errorf("%w: tenant %q key %q",
				ErrIdempotencyConflict, rec.TenantID, rec.IdempotencyKey)
		}
		return Acceptance{}, fmt.Errorf("event id %s collided with a different request", rec.ID)
	}
	if published {
		if err := tx.Commit(ctx); err != nil {
			return Acceptance{}, fmt.Errorf("finish deduplicated event %s: %w", accepted.ID, err)
		}
		return Acceptance{Record: accepted, Deduplicated: true}, nil
	}

	if err := publish(ctx, accepted); err != nil {
		return Acceptance{}, fmt.Errorf("%w for event %s: %w", ErrPublishFailed, accepted.ID, err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE relay_events SET published_at = COALESCE(published_at, now()) WHERE id = $1`,
		accepted.ID,
	); err != nil {
		return Acceptance{}, fmt.Errorf("mark event %s published: %w", accepted.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Acceptance{}, fmt.Errorf("commit event %s publication: %w", accepted.ID, err)
	}
	return Acceptance{Record: accepted}, nil
}

func (s *Store) acquireAcceptanceSlot(ctx context.Context) error {
	select {
	case s.acceptanceSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for event acceptance capacity: %w", ctx.Err())
	}
}

func lockAcceptedEvent(ctx context.Context, tx pgx.Tx, candidate event.Record) (event.Record, bool, bool, error) {
	var (
		accepted  event.Record
		dataRaw   string
		published bool
		matches   bool
		err       error
	)
	if candidate.IdempotencyKey == "" {
		err = tx.QueryRow(ctx, `
			SELECT id, tenant_id, event_type, data_raw, COALESCE(idempotency_key, ''), occurred_at,
			       published_at IS NOT NULL,
			       tenant_id = $2 AND event_type = $3 AND data = $4::jsonb
			FROM relay_events
			WHERE id = $1
			FOR UPDATE`,
			candidate.ID, candidate.TenantID, candidate.Type, candidate.Data,
		).Scan(
			&accepted.ID, &accepted.TenantID, &accepted.Type, &dataRaw,
			&accepted.IdempotencyKey, &accepted.OccurredAt, &published, &matches,
		)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT id, tenant_id, event_type, data_raw, COALESCE(idempotency_key, ''), occurred_at,
			       published_at IS NOT NULL,
			       event_type = $3 AND data = $4::jsonb
			FROM relay_events
			WHERE tenant_id = $1 AND idempotency_key = $2
			  AND idempotency_claimed_at IS NOT NULL
			FOR UPDATE`,
			candidate.TenantID, candidate.IdempotencyKey, candidate.Type, candidate.Data,
		).Scan(
			&accepted.ID, &accepted.TenantID, &accepted.Type, &dataRaw,
			&accepted.IdempotencyKey, &accepted.OccurredAt, &published, &matches,
		)
	}
	if err != nil {
		return event.Record{}, false, false, fmt.Errorf("lock accepted event: %w", err)
	}
	accepted.Data = []byte(dataRaw)
	return accepted, published, matches, nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// RecordAttempt appends an attempt under a lock on its event row. The lock gives
// each event-and-subscription pair a gap-free sequence across Kafka redelivery
// without trusting an in-memory counter.
func (s *Store) RecordAttempt(ctx context.Context, eventID string, attempt Attempt) error {
	if s == nil || s.pool == nil {
		return ErrNoPool
	}
	if err := validateAttempt(eventID, attempt); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin attempt history transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var lockedID string
	if err := tx.QueryRow(ctx,
		`SELECT id FROM relay_events WHERE id = $1 FOR UPDATE`, eventID,
	).Scan(&lockedID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrEventNotFound, eventID)
		}
		return fmt.Errorf("lock event %s: %w", eventID, err)
	}

	var number int
	err = tx.QueryRow(ctx, `
		INSERT INTO relay_delivery_attempts (
			event_id, subscription_id, subscription_url, attempt_number,
			started_at, finished_at, http_status, transport_error, outcome
		)
		SELECT $1, $2, $3, COALESCE(MAX(attempt_number), 0) + 1,
		       $4, $5, $6, NULLIF($7, ''), $8
		FROM relay_delivery_attempts
		WHERE event_id = $1 AND subscription_id = $2
		RETURNING attempt_number`,
		eventID, attempt.SubscriptionID, attempt.SubscriptionURL,
		attempt.StartedAt, attempt.FinishedAt, attempt.HTTPStatus,
		attempt.TransportError, attempt.Outcome,
	).Scan(&number)
	if err != nil {
		return fmt.Errorf("insert attempt for event %s: %w", eventID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit attempt for event %s: %w", eventID, err)
	}
	return nil
}

// Attempts returns an event's attempts in the durable sequence assigned at
// insert time. A known event with no attempts returns an empty JSON-ready slice.
func (s *Store) Attempts(ctx context.Context, eventID string) ([]Attempt, error) {
	if s == nil || s.pool == nil {
		return nil, ErrNoPool
	}

	var exists bool
	if err := s.pool.QueryRow(ctx,
		`SELECT true FROM relay_events WHERE id = $1`, eventID,
	).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrEventNotFound, eventID)
		}
		return nil, fmt.Errorf("find event %s: %w", eventID, err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT subscription_id, subscription_url, attempt_number,
		       started_at, finished_at, http_status,
		       COALESCE(transport_error, ''), outcome
		FROM relay_delivery_attempts
		WHERE event_id = $1
		ORDER BY started_at, id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("query attempts for event %s: %w", eventID, err)
	}
	defer rows.Close()

	attempts := make([]Attempt, 0)
	for rows.Next() {
		var attempt Attempt
		if err := rows.Scan(
			&attempt.SubscriptionID, &attempt.SubscriptionURL, &attempt.AttemptNumber,
			&attempt.StartedAt, &attempt.FinishedAt, &attempt.HTTPStatus,
			&attempt.TransportError, &attempt.Outcome,
		); err != nil {
			return nil, fmt.Errorf("scan attempt for event %s: %w", eventID, err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read attempts for event %s: %w", eventID, err)
	}
	return attempts, nil
}

func validateAttempt(eventID string, attempt Attempt) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("history: blank event id")
	}
	if attempt.SubscriptionID <= 0 {
		return fmt.Errorf("history: subscription id %d must be positive", attempt.SubscriptionID)
	}
	if strings.TrimSpace(attempt.SubscriptionURL) == "" {
		return errors.New("history: blank subscription url")
	}
	if attempt.StartedAt.IsZero() || attempt.FinishedAt.IsZero() || attempt.FinishedAt.Before(attempt.StartedAt) {
		return errors.New("history: invalid attempt timestamps")
	}
	if (attempt.HTTPStatus == nil) == (attempt.TransportError == "") {
		return errors.New("history: attempt needs exactly one HTTP status or transport error")
	}
	switch attempt.Outcome {
	case OutcomeDelivered, OutcomeRetrying, OutcomeExhausted, OutcomeInterrupted:
		return nil
	default:
		return fmt.Errorf("history: invalid attempt outcome %q", attempt.Outcome)
	}
}

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
	ErrNoPool        = errors.New("history: no database pool")
	ErrEventNotFound = errors.New("history: event not found")
)

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
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// CreateEvent persists an event before ingest publishes it to Kafka. That
// ordering ensures a fast consumer can always attach an attempt to its event.
func (s *Store) CreateEvent(ctx context.Context, rec event.Record) error {
	if s == nil || s.pool == nil {
		return ErrNoPool
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO relay_events
			(id, tenant_id, event_type, data, idempotency_key, occurred_at)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), $6)`,
		rec.ID, rec.TenantID, rec.Type, rec.Data, rec.IdempotencyKey, rec.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert event %s: %w", rec.ID, err)
	}
	return nil
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

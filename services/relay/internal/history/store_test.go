package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
)

func TestZeroStoreDoesNotPanic(t *testing.T) {
	t.Parallel()

	var store *Store
	if err := store.CreateEvent(context.Background(), event.Record{}); !errors.Is(err, ErrNoPool) {
		t.Errorf("nil CreateEvent error = %v, want ErrNoPool", err)
	}
	if _, err := (&Store{}).Attempts(context.Background(), "evt_x"); !errors.Is(err, ErrNoPool) {
		t.Errorf("empty Attempts error = %v, want ErrNoPool", err)
	}
}

func TestValidateAttempt(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	status := 200
	valid := Attempt{
		SubscriptionID:  1,
		SubscriptionURL: "https://example.test/hook",
		StartedAt:       now,
		FinishedAt:      now.Add(time.Millisecond),
		HTTPStatus:      &status,
		Outcome:         OutcomeDelivered,
	}
	if err := validateAttempt("evt_x", valid); err != nil {
		t.Fatalf("valid attempt: %v", err)
	}

	withBoth := valid
	withBoth.TransportError = "connection reset"
	if err := validateAttempt("evt_x", withBoth); err == nil {
		t.Error("attempt with an HTTP status and transport error was accepted")
	}
	badOutcome := valid
	badOutcome.Outcome = "maybe"
	if err := validateAttempt("evt_x", badOutcome); err == nil {
		t.Error("attempt with an unknown outcome was accepted")
	}
}

// Integration. It skips when Postgres is down; when Postgres is reachable the
// relay bootstrap schema is part of the contract and a missing table is a test
// failure rather than a skip.
func TestStoreAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://platform:platform@localhost:5432/platform?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("no postgres pool (%v); run `make up` and `make seed`", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable (%v); run `make up` and `make seed`", err)
	}

	store := New(pool)
	eventID := fmt.Sprintf("evt_history_test_%d", time.Now().UnixNano())
	rec := event.Record{
		ID:         eventID,
		TenantID:   "history-test",
		Type:       "history.test",
		Data:       json.RawMessage(`{"n":1}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := store.CreateEvent(ctx, rec); err != nil {
		t.Fatalf("CreateEvent: %v; has local/bootstrap/relay-db.sh been run?", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM relay_events WHERE id = $1`, eventID); err != nil {
			t.Errorf("cleanup event: %v", err)
		}
	})

	empty, err := store.Attempts(ctx, eventID)
	if err != nil {
		t.Fatalf("Attempts before delivery: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("Attempts before delivery = %#v, want []", empty)
	}

	status := 500
	firstStart := time.Now().UTC()
	if err := store.RecordAttempt(ctx, eventID, Attempt{
		SubscriptionID:  41,
		SubscriptionURL: "https://example.test/hook",
		StartedAt:       firstStart,
		FinishedAt:      firstStart.Add(time.Millisecond),
		HTTPStatus:      &status,
		Outcome:         OutcomeRetrying,
	}); err != nil {
		t.Fatalf("RecordAttempt(first): %v", err)
	}
	secondStart := firstStart.Add(2 * time.Millisecond)
	if err := store.RecordAttempt(ctx, eventID, Attempt{
		SubscriptionID:  41,
		SubscriptionURL: "https://example.test/hook",
		StartedAt:       secondStart,
		FinishedAt:      secondStart.Add(time.Millisecond),
		TransportError:  "connection reset",
		Outcome:         OutcomeExhausted,
	}); err != nil {
		t.Fatalf("RecordAttempt(second): %v", err)
	}

	got, err := store.Attempts(ctx, eventID)
	if err != nil {
		t.Fatalf("Attempts after delivery: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Attempts returned %d rows, want 2", len(got))
	}
	if got[0].AttemptNumber != 1 || got[0].HTTPStatus == nil || *got[0].HTTPStatus != 500 ||
		got[0].Outcome != OutcomeRetrying {
		t.Errorf("first attempt = %+v, want retrying HTTP 500 number 1", got[0])
	}
	if got[1].AttemptNumber != 2 || got[1].TransportError != "connection reset" ||
		got[1].Outcome != OutcomeExhausted {
		t.Errorf("second attempt = %+v, want exhausted transport error number 2", got[1])
	}

	if _, err := store.Attempts(ctx, eventID+"_missing"); !errors.Is(err, ErrEventNotFound) {
		t.Errorf("unknown event error = %v, want ErrEventNotFound", err)
	}
}

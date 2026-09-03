package history

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
)

func TestZeroStoreDoesNotPanic(t *testing.T) {
	t.Parallel()

	var store *Store
	if _, err := store.AcceptEvent(context.Background(), event.Record{}, func(context.Context, event.Record) error {
		return nil
	}); !errors.Is(err, ErrNoPool) {
		t.Errorf("nil AcceptEvent error = %v, want ErrNoPool", err)
	}
	if _, err := (&Store{}).Attempts(context.Background(), "evt_x"); !errors.Is(err, ErrNoPool) {
		t.Errorf("empty Attempts error = %v, want ErrNoPool", err)
	}
}

func TestNewReservesOnePoolConnectionForOtherHistoryWork(t *testing.T) {
	t.Parallel()

	config, err := pgxpool.ParseConfig("postgres://platform:platform@localhost:5432/platform?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 3
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	store := New(pool)
	if got := cap(store.acceptanceSlots); got != 2 {
		t.Errorf("acceptance slots = %d, want 2 for a 3-connection pool", got)
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
	testRun := time.Now().UnixNano()
	tenantID := fmt.Sprintf("history-test-%d", testRun)
	eventID := fmt.Sprintf("evt_history_test_%d", testRun)
	rec := event.Record{
		ID:             eventID,
		TenantID:       tenantID,
		Type:           "history.test",
		Data:           json.RawMessage("{\n  \"b\": 2, \"a\": 1\n}"),
		IdempotencyKey: "history-key",
		OccurredAt:     time.Now().UTC(),
	}
	var publishCalls atomic.Int32
	accepted, err := store.AcceptEvent(ctx, rec, func(_ context.Context, got event.Record) error {
		publishCalls.Add(1)
		if got.ID != rec.ID {
			t.Errorf("publisher event id = %q, want %q", got.ID, rec.ID)
		}
		if string(got.Data) != string(rec.Data) {
			t.Errorf("publisher data = %q, want original bytes %q", got.Data, rec.Data)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("AcceptEvent: %v; has local/bootstrap/relay-db.sh been run?", err)
	}
	if accepted.Record.ID != eventID || accepted.Deduplicated {
		t.Fatalf("first acceptance = %+v, want new event %s", accepted, eventID)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM relay_events WHERE tenant_id LIKE $1`, tenantID+"%"); err != nil {
			t.Errorf("cleanup event: %v", err)
		}
	})

	repeat := rec
	repeat.ID = eventID + "_repeat"
	repeat.Data = json.RawMessage(`{"a":1,"b":2}`)
	repeat.OccurredAt = rec.OccurredAt.Add(time.Second)
	deduplicated, err := store.AcceptEvent(ctx, repeat, func(context.Context, event.Record) error {
		publishCalls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("AcceptEvent(repeat): %v", err)
	}
	if deduplicated.Record.ID != eventID || !deduplicated.Deduplicated {
		t.Errorf("repeat acceptance = %+v, want original event %s marked deduplicated", deduplicated, eventID)
	}
	if string(deduplicated.Record.Data) != string(rec.Data) {
		t.Errorf("deduplicated data = %q, want original bytes %q", deduplicated.Record.Data, rec.Data)
	}
	if got := publishCalls.Load(); got != 1 {
		t.Errorf("publisher called %d times after repeat, want 1", got)
	}

	conflict := repeat
	conflict.ID = eventID + "_conflict"
	conflict.Data = json.RawMessage(`{"n":2}`)
	if _, err := store.AcceptEvent(ctx, conflict, func(context.Context, event.Record) error {
		return nil
	}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("conflicting request error = %v, want ErrIdempotencyConflict", err)
	}

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

	t.Run("concurrent requests publish once", func(t *testing.T) {
		key := "concurrent-key"
		var calls atomic.Int32
		results := make([]Acceptance, 2)
		errs := make([]error, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range results {
			candidate := event.Record{
				ID:             fmt.Sprintf("evt_concurrent_%d_%d", testRun, i),
				TenantID:       tenantID,
				Type:           "history.concurrent",
				Data:           json.RawMessage(`{"n":1}`),
				IdempotencyKey: key,
				OccurredAt:     time.Now().UTC(),
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[i], errs[i] = store.AcceptEvent(ctx, candidate, func(context.Context, event.Record) error {
					calls.Add(1)
					time.Sleep(50 * time.Millisecond)
					return nil
				})
			}()
		}
		close(start)
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("concurrent request %d: %v", i, err)
			}
		}
		if results[0].Record.ID != results[1].Record.ID {
			t.Errorf("concurrent event ids = %q and %q, want one id", results[0].Record.ID, results[1].Record.ID)
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("concurrent publisher calls = %d, want 1", got)
		}
	})

	t.Run("failed publish stays pending until recovery", func(t *testing.T) {
		candidate := event.Record{
			ID:             fmt.Sprintf("evt_recovery_%d", testRun),
			TenantID:       tenantID,
			Type:           "history.recovery",
			Data:           json.RawMessage(`{"z":0, "n":1}`),
			IdempotencyKey: "recovery-key",
			OccurredAt:     time.Now().UTC(),
		}
		brokerErr := errors.New("broker unavailable")
		if _, err := store.AcceptEvent(ctx, candidate, func(context.Context, event.Record) error {
			return brokerErr
		}); !errors.Is(err, ErrPublishFailed) || !errors.Is(err, brokerErr) {
			t.Fatalf("failed publish error = %v, want ErrPublishFailed and broker error", err)
		}

		var published bool
		if err := pool.QueryRow(ctx,
			`SELECT published_at IS NOT NULL FROM relay_events WHERE id = $1`, candidate.ID,
		).Scan(&published); err != nil {
			t.Fatalf("query pending event: %v", err)
		}
		if published {
			t.Fatal("failed publication left a successful idempotency claim")
		}

		retry := candidate
		retry.ID += "_retry"
		retry.Data = json.RawMessage(`{"n":1,"z":0}`)
		var recoveredData string
		recovered, err := store.AcceptEvent(ctx, retry, func(_ context.Context, got event.Record) error {
			recoveredData = string(got.Data)
			return nil
		})
		if err != nil {
			t.Fatalf("recover pending event: %v", err)
		}
		if recovered.Record.ID != candidate.ID || recovered.Deduplicated {
			t.Errorf("recovery acceptance = %+v, want original pending event", recovered)
		}
		if recoveredData != string(candidate.Data) {
			t.Errorf("recovery published data = %q, want original bytes %q", recoveredData, candidate.Data)
		}
	})

	t.Run("historical duplicate keys do not claim the new contract", func(t *testing.T) {
		key := "historical-duplicate-key"
		for i := 1; i <= 2; i++ {
			if _, err := pool.Exec(ctx, `
				INSERT INTO relay_events
					(id, tenant_id, event_type, data, data_raw, idempotency_key,
					 occurred_at, published_at)
				VALUES ($1, $2, 'history.old', '{"old":true}', '{"old":true}', $3, now(), now())`,
				fmt.Sprintf("evt_historical_%d_%d", testRun, i), tenantID, key,
			); err != nil {
				t.Fatalf("insert historical duplicate %d: %v", i, err)
			}
		}

		candidate := event.Record{
			ID:             fmt.Sprintf("evt_historical_claim_%d", testRun),
			TenantID:       tenantID,
			Type:           "history.new",
			Data:           json.RawMessage(`{"new":true}`),
			IdempotencyKey: key,
			OccurredAt:     time.Now().UTC(),
		}
		accepted, err := store.AcceptEvent(ctx, candidate, func(context.Context, event.Record) error { return nil })
		if err != nil {
			t.Fatalf("accept new claim beside historical duplicates: %v", err)
		}
		if accepted.Record.ID != candidate.ID || accepted.Deduplicated {
			t.Errorf("acceptance = %+v, want new claimed event %s", accepted, candidate.ID)
		}

		var claimed int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM relay_events
			WHERE tenant_id = $1 AND idempotency_key = $2
			  AND idempotency_claimed_at IS NOT NULL`, tenantID, key,
		).Scan(&claimed); err != nil {
			t.Fatalf("count new claims: %v", err)
		}
		if claimed != 1 {
			t.Errorf("claimed rows = %d, want 1", claimed)
		}
	})

	t.Run("blank keys remain independent", func(t *testing.T) {
		for keyNumber, key := range []string{"", " \t "} {
			for eventNumber := 1; eventNumber <= 2; eventNumber++ {
				id := fmt.Sprintf("evt_blank_%d_%d_%d", testRun, keyNumber, eventNumber)
				candidate := event.Record{
					ID: id, TenantID: tenantID, Type: "history.blank",
					Data: json.RawMessage(`{"n":1}`), IdempotencyKey: key, OccurredAt: time.Now().UTC(),
				}
				accepted, err := store.AcceptEvent(ctx, candidate, func(context.Context, event.Record) error { return nil })
				if err != nil {
					t.Fatalf("accept blank-key event: %v", err)
				}
				if accepted.Record.ID != id || accepted.Deduplicated || accepted.Record.IdempotencyKey != "" {
					t.Errorf("blank-key acceptance = %+v, want independent normalized event %s", accepted, id)
				}
			}
		}
	})
}

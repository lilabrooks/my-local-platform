package delivery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
)

func testRecord() event.Record {
	return event.Record{
		ID:         "evt_abc",
		TenantID:   "acme",
		Type:       "invoice.paid",
		Data:       json.RawMessage(`{"amount":100}`),
		OccurredAt: time.Now().UTC(),
	}
}

// newTestDeliverer skips the real waits so a retry test does not sit out the
// schedule. The delays themselves are covered in internal/config.
func newTestDeliverer(t *testing.T, spec string) *Deliverer {
	t.Helper()
	s, err := config.ParseRetrySchedule(spec, false)
	if err != nil {
		t.Fatalf("parse schedule: %v", err)
	}
	d := NewDeliverer(s, 2*time.Second)
	d.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return d
}

func TestDeliverSucceedsFirstTry(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := newTestDeliverer(t, "1s,2s").Deliver(context.Background(),
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !out.Delivered {
		t.Errorf("Delivered = false, reason %q", out.Reason)
	}
	if out.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", out.Attempts)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("subscriber called %d times, want 1", got)
	}
}

func TestDeliverRetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := newTestDeliverer(t, "1s,2s,4s").Deliver(context.Background(),
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !out.Delivered {
		t.Errorf("Delivered = false after a recovering subscriber, reason %q", out.Reason)
	}
	if out.Attempts != 3 {
		t.Errorf("Attempts = %d, want 3", out.Attempts)
	}
	if out.Reason != "" {
		t.Errorf("Reason = %q, want empty on success", out.Reason)
	}
}

func TestDeliverExhaustsBudget(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	// 3 delays means 4 attempts.
	out, err := newTestDeliverer(t, "1s,2s,4s").Deliver(context.Background(),
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if err != nil {
		t.Fatalf("Deliver returned an error for an exhausted budget; that is a normal outcome: %v", err)
	}
	if out.Delivered {
		t.Error("Delivered = true against a subscriber that never succeeded")
	}
	if out.Attempts != 4 {
		t.Errorf("Attempts = %d, want 4 (3 delays + the first attempt)", out.Attempts)
	}
	if got := calls.Load(); got != 4 {
		t.Errorf("subscriber called %d times, want 4", got)
	}
	if !strings.Contains(out.Reason, "gave up") || !strings.Contains(out.Reason, "500") {
		t.Errorf("Reason = %q, want it to say it gave up and name the status", out.Reason)
	}
}

// A redirect is not an acknowledgement that the event was processed.
func TestDeliverTreats3xxAsFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://example.test/elsewhere")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer srv.Close()

	// NewDeliverer must already refuse to follow redirects; a subscriber that
	// could redirect relay would have it POST signed payloads anywhere.
	out, err := newTestDeliverer(t, "1s").Deliver(context.Background(),
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if out.Delivered {
		t.Error("a 301 was treated as a successful delivery")
	}
}

// The signature must verify, the id must be stable across retries, and the
// timestamp must be refreshed per attempt so a late retry is not a replay.
func TestDeliverSignsEveryAttempt(t *testing.T) {
	t.Parallel()

	type seen struct {
		id, ts, sig string
		body        []byte
	}
	var (
		mu    sync.Mutex
		got   []seen
		calls atomic.Int32
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, seen{
			id:   r.Header.Get(HeaderID),
			ts:   r.Header.Get(HeaderTimestamp),
			sig:  r.Header.Get(HeaderSignature),
			body: body,
		})
		mu.Unlock()
		if calls.Add(1) < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := testRecord()
	if _, err := newTestDeliverer(t, "1s,2s").Deliver(context.Background(),
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "the-secret"}, rec); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("saw %d attempts, want 2", len(got))
	}
	for i, g := range got {
		if g.id != rec.ID {
			t.Errorf("attempt %d webhook-id = %q, want %q -- it must be stable so subscribers can dedupe", i+1, g.id, rec.ID)
		}
		want := Sign([]byte("the-secret"), g.id, parseUnix(t, g.ts), g.body)
		if g.sig != want {
			t.Errorf("attempt %d signature does not verify against its own headers and body", i+1)
		}
	}

	// The body is the specification envelope and nothing else.
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(got[0].body, &payload); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if len(payload) != 2 || payload["type"] == nil || payload["data"] == nil {
		t.Errorf("body = %s, want exactly type and data", got[0].body)
	}
}

func parseUnix(t *testing.T, s string) time.Time {
	t.Helper()
	secs, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatalf("timestamp %q: %v", s, err)
	}
	return time.Unix(secs, 0)
}

// A cancelled context stops delivery and is reported as an error, so the
// consumer knows not to commit.
func TestDeliverStopsOnCancellation(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newTestDeliverer(t, "1s,2s").Deliver(ctx,
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if err == nil {
		t.Fatal("Deliver returned nil for a cancelled context; the consumer would commit a record it did not finish")
	}
}

// An unreachable subscriber is a failure like any other, not a panic.
func TestDeliverHandlesConnectionRefused(t *testing.T) {
	t.Parallel()

	out, err := newTestDeliverer(t, "1s").Deliver(context.Background(),
		subscriptions.Subscription{ID: 1, URL: "http://127.0.0.1:1/nothing", Secret: "s"}, testRecord())
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if out.Delivered {
		t.Error("Delivered = true against a refused connection")
	}
	if out.LastStatus != 0 {
		t.Errorf("LastStatus = %d, want 0 when no response arrived", out.LastStatus)
	}
}

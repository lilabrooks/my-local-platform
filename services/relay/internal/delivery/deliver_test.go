package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/history"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
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
	d := NewDeliverer(s, 2*time.Second, nil)
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

	recorder := &fakeAttemptRecorder{}
	deliverer := newTestDeliverer(t, "1s,2s,4s")
	deliverer.recorder = recorder
	out, err := deliverer.Deliver(context.Background(),
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
	recorded := recorder.attempts()
	if len(recorded) != 3 {
		t.Fatalf("recorded %d attempts, want 3", len(recorded))
	}
	wantOutcomes := []string{history.OutcomeRetrying, history.OutcomeRetrying, history.OutcomeDelivered}
	for i, want := range wantOutcomes {
		if recorded[i].eventID != "evt_abc" || recorded[i].attempt.AttemptNumber != i+1 ||
			recorded[i].attempt.Outcome != want {
			t.Errorf("recorded attempt %d = %+v, want event evt_abc number %d outcome %s",
				i, recorded[i], i+1, want)
		}
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
	recorder := &fakeAttemptRecorder{}
	deliverer := newTestDeliverer(t, "1s,2s,4s")
	deliverer.recorder = recorder
	out, err := deliverer.Deliver(context.Background(),
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
	recorded := recorder.attempts()
	if len(recorded) != 4 || recorded[3].attempt.Outcome != history.OutcomeExhausted {
		t.Errorf("recorded attempts = %+v, want four with an exhausted final attempt", recorded)
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

func TestInterruptedAttemptIsRecordedWithDetachedContext(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	recorder := &fakeAttemptRecorder{}
	deliverer := newTestDeliverer(t, "1s")
	deliverer.recorder = recorder
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err := deliverer.Deliver(ctx,
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Deliver error = %v, want context deadline exceeded", err)
	}
	recorded := recorder.attempts()
	if len(recorded) != 1 {
		t.Fatalf("recorded %d attempts, want 1", len(recorded))
	}
	if recorded[0].attempt.Outcome != history.OutcomeInterrupted ||
		recorded[0].attempt.TransportError == "" {
		t.Errorf("interrupted attempt = %+v, want transport error and interrupted outcome", recorded[0])
	}
}

func TestInterruptedAttemptReportsDetachedHistoryWriteFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	writeErr := errors.New("postgres unavailable")
	deliverer := newTestDeliverer(t, "1s")
	deliverer.recorder = &fakeAttemptRecorder{fail: writeErr}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	_, err := deliverer.Deliver(ctx,
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Deliver error = %v, want context deadline exceeded", err)
	}
	if !errors.Is(err, writeErr) {
		t.Fatalf("Deliver error = %v, want detached history write failure", err)
	}
}

// An unreachable subscriber is a failure like any other, not a panic.
func TestDeliverHandlesConnectionRefused(t *testing.T) {
	t.Parallel()

	recorder := &fakeAttemptRecorder{}
	deliverer := newTestDeliverer(t, "1s")
	deliverer.recorder = recorder
	out, err := deliverer.Deliver(context.Background(),
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
	recorded := recorder.attempts()
	if len(recorded) != 2 || recorded[0].attempt.HTTPStatus != nil || recorded[0].attempt.TransportError == "" {
		t.Errorf("recorded transport failures = %+v, want errors without HTTP status", recorded)
	}
}

func TestDeliverCountsAttemptsByStatusClass(t *testing.T) {
	// Not parallel: it reads package-level counters in internal/metrics, and a
	// concurrent delivery test would land in the same series.
	attempts := func(class string) float64 {
		return testutil.ToFloat64(metrics.DeliveryAttempts.WithLabelValues(class))
	}
	before5xx, before2xx, beforeErr := attempts("5xx"), attempts("2xx"), attempts("error")

	// Two failures then a success: three attempts, two classes.
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := newTestDeliverer(t, "1ms,1ms,1ms").Deliver(context.Background(),
		subscriptions.Subscription{ID: 1, URL: srv.URL, Secret: "s"}, testRecord())
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if !out.Delivered {
		t.Fatalf("Delivered = false after a 200 on the third attempt: %s", out.Reason)
	}

	if got := attempts("5xx") - before5xx; got != 2 {
		t.Errorf("5xx attempts moved by %v, want 2", got)
	}
	if got := attempts("2xx") - before2xx; got != 1 {
		t.Errorf("2xx attempts moved by %v, want 1", got)
	}

	// A refused connection is counted too, under "error" rather than 5xx.
	// Attempts that produce no response at all would otherwise be invisible:
	// the counter would move only when a server answered, so a subscriber
	// whose host had gone away would look like no traffic rather than trouble.
	//
	// One delay is a budget of two attempts (MaxAttempts is len(delays)+1), and
	// both are refused, so the class gains two.
	if _, err := newTestDeliverer(t, "1ms").Deliver(context.Background(),
		subscriptions.Subscription{ID: 2, URL: "http://127.0.0.1:1/nothing", Secret: "s"}, testRecord()); err != nil {
		t.Fatalf("Deliver against a refused connection: %v", err)
	}
	if got := attempts("error") - beforeErr; got != 2 {
		t.Errorf("error-class attempts moved by %v, want 2", got)
	}
}

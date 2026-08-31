package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
)

// --- fakes ------------------------------------------------------------------

type fakeReader struct {
	mu        sync.Mutex
	queue     []kafka.Message
	committed []kafka.Message
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queue) == 0 {
		// Nothing left: behave like a cancelled fetch so Run returns.
		return kafka.Message{}, context.Canceled
	}
	m := f.queue[0]
	f.queue = f.queue[1:]
	return m, ctx.Err()
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeReader) Close() error { return nil }

func (f *fakeReader) commits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.committed)
}

type fakeDLQ struct {
	mu   sync.Mutex
	got  []kafka.Message
	fail error
}

func (f *fakeDLQ) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.got = append(f.got, msgs...)
	return nil
}

func (f *fakeDLQ) letters(t *testing.T) []DeadLetter {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]DeadLetter, 0, len(f.got))
	for _, m := range f.got {
		var dl DeadLetter
		if err := json.Unmarshal(m.Value, &dl); err != nil {
			t.Fatalf("dead letter is not decodable: %v", err)
		}
		out = append(out, dl)
	}
	return out
}

type fakeSubs struct {
	subs []subscriptions.Subscription
	fail error
}

func (f fakeSubs) ForTenant(context.Context, string) ([]subscriptions.Subscription, error) {
	return f.subs, f.fail
}

// --- helpers ----------------------------------------------------------------

func recordMessage(t *testing.T, rec event.Record) kafka.Message {
	t.Helper()
	v, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return kafka.Message{Partition: 3, Offset: 42, Key: rec.Key(), Value: v}
}

func newConsumer(t *testing.T, r Reader, dlq Producer, subs SubscriptionSource) *Consumer {
	t.Helper()
	s, err := config.ParseRetrySchedule("1s", false) // 2 attempts
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	d := NewDeliverer(s, 2*time.Second)
	d.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return NewConsumer(r, dlq, subs, d, 30*time.Second, slog.New(slog.DiscardHandler))
}

func alwaysOK(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func alwaysFails(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

// --- tests ------------------------------------------------------------------

func TestSuccessfulDeliveryCommitsWithoutDeadLettering(t *testing.T) {
	t.Parallel()

	ok := alwaysOK(t)
	defer ok.Close()

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: ok.URL, Secret: "s"}}})

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times, want 1", got)
	}
	if got := len(dlq.letters(t)); got != 0 {
		t.Errorf("wrote %d dead letters for a successful delivery, want 0", got)
	}
}

func TestExhaustedDeliveryIsDeadLetteredThenCommitted(t *testing.T) {
	t.Parallel()

	bad := alwaysFails(t)
	defer bad.Close()

	rec := testRecord()
	r := &fakeReader{queue: []kafka.Message{recordMessage(t, rec)}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 7, URL: bad.URL, Secret: "s"}}})

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	letters := dlq.letters(t)
	if len(letters) != 1 {
		t.Fatalf("wrote %d dead letters, want 1", len(letters))
	}
	dl := letters[0]
	if dl.Record.ID != rec.ID {
		t.Errorf("dead letter carries record %q, want %q -- it must be replayable", dl.Record.ID, rec.ID)
	}
	if dl.SubscriptionID != 7 || dl.URL != bad.URL {
		t.Errorf("dead letter does not identify the subscription: %+v", dl)
	}
	if dl.LastStatus != http.StatusInternalServerError {
		t.Errorf("LastStatus = %d, want 500", dl.LastStatus)
	}
	if dl.Reason == "" {
		t.Error("dead letter has no reason; whoever reads this later needs one")
	}
	// Only after the failure is recorded may the offset advance.
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times, want 1", got)
	}
}

// The signing secret must never reach the dead-letter topic, which is readable
// by anything with access to Kafka.
func TestDeadLetterDoesNotCarryTheSigningSecret(t *testing.T) {
	t.Parallel()

	bad := alwaysFails(t)
	defer bad.Close()

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{
		{ID: 7, URL: bad.URL, Secret: "super-secret-signing-key"},
	}})

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	for _, m := range dlq.got {
		if strings.Contains(string(m.Value), "super-secret-signing-key") {
			t.Fatalf("dead letter leaked the signing secret: %s", m.Value)
		}
	}
}

// The whole point of committing last: if the failure could not be recorded, the
// offset must not advance, or the event is silently lost.
func TestDLQFailureBlocksTheCommit(t *testing.T) {
	t.Parallel()

	bad := alwaysFails(t)
	defer bad.Close()

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{fail: errors.New("dlq broker unreachable")}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: bad.URL, Secret: "s"}}})

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("Run returned nil when the dead-letter write failed")
	}
	if got := r.commits(); got != 0 {
		t.Errorf("committed %d times after a failed dead-letter write, want 0", got)
	}
}

// The most important property this consumer has is that an offset commits only
// when every subscriber is finished. handle used to record "something failed"
// as a bool and then rebuild an error with context.Cause(ctx) -- so a delivery
// error that was NOT a cancellation produced a nil cause, handle returned nil,
// and the record committed as though every subscriber had succeeded. Silent
// data loss, guarded only by an invariant maintained in a different function:
// Deliver happening to return nothing but ctx.Err().
//
// That invariant is not enforced anywhere, and it is one line from being false
// -- Deliver returns d.sleep's error verbatim (deliver.go), so any sleep that
// fails for its own reasons walks straight into this path. This test makes the
// error non-context and asserts the record is NOT committed.
//
// See issue #24.
func TestNonContextDeliveryErrorBlocksTheCommit(t *testing.T) {
	t.Parallel()

	bad := alwaysFails(t) // fails, so delivery reaches the retry sleep
	defer bad.Close()

	boom := errors.New("clock went backwards")

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: bad.URL, Secret: "s"}}})
	// Not a context error, and the context is never cancelled -- so
	// context.Cause(ctx) is nil and the old code returned nil from handle.
	c.deliverer.sleep = func(context.Context, time.Duration) error { return boom }

	err := c.Run(context.Background())

	if got := r.commits(); got != 0 {
		t.Errorf("committed %d times after a non-context delivery error, want 0 -- "+
			"the record was treated as fully delivered and is now lost", got)
	}
	if err == nil {
		t.Fatal("Run returned nil after a delivery error; the failure was swallowed")
	}
	if !errors.Is(err, boom) {
		t.Errorf("Run returned %v, want it to wrap %v -- reconstructing an error "+
			"from the context loses what actually went wrong", err, boom)
	}
}

// SIGTERM stops fetching and drops readiness, then lets the record already in
// hand finish and commit. This is what gives terminationGracePeriodSeconds a
// real job instead of leaving it as unused manifest arithmetic.
func TestCancellationDuringDeliveryDrainsAndCommits(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: ok.URL, Secret: "s"}}})

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	<-started
	cancel()

	deadline := time.Now().Add(time.Second)
	for c.Ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Ready() {
		t.Fatal("consumer stayed ready after shutdown began")
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before the in-flight record was released: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Run after drain: %v", err)
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times after draining, want 1", got)
	}
	if got := len(dlq.letters(t)); got != 0 {
		t.Errorf("wrote %d dead letters for a successful drain, want 0", got)
	}
}

func TestRecordTimeoutBoundsWorkAndLeavesOffsetUncommitted(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		blocked.Close()
	})

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: blocked.URL, Secret: "s"}}})
	c.recordTimeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background()) }()
	<-started
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the record deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run returned %v, want record deadline exceeded", err)
	}
	if got := r.commits(); got != 0 {
		t.Errorf("committed %d times after the record deadline, want 0", got)
	}
}

func TestCancellationDrainTimeoutStopsCleanlyWithoutCommit(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		blocked.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: blocked.URL, Secret: "s"}}})
	c.recordTimeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	<-started
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v when the shutdown drain expired, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after the shutdown drain deadline")
	}
	if c.Ready() {
		t.Fatal("consumer stayed ready after shutdown drain expired")
	}
	if got := r.commits(); got != 0 {
		t.Errorf("committed %d times after the shutdown drain expired, want 0", got)
	}
}

// A database blip is not the event's fault; the record must come back.
func TestSubscriptionLookupFailureBlocksTheCommit(t *testing.T) {
	t.Parallel()

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{fail: errors.New("connection reset")})

	if err := c.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil when the subscription lookup failed")
	}
	if got := r.commits(); got != 0 {
		t.Errorf("committed %d times after a failed lookup, want 0", got)
	}
	if got := len(dlq.letters(t)); got != 0 {
		t.Errorf("wrote %d dead letters for a lookup failure, want 0 -- the event is not at fault", got)
	}
}

// An event nobody subscribes to is delivered nowhere, successfully.
func TestNoSubscribersCommits(t *testing.T) {
	t.Parallel()

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{})

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times, want 1", got)
	}
	if got := len(dlq.letters(t)); got != 0 {
		t.Errorf("wrote %d dead letters, want 0", got)
	}
}

// This is the acceptance criterion from issue #6: one failing subscriber must
// not stop delivery to a healthy subscriber of the same event.
func TestOneFailingSubscriberDoesNotBlockAHealthyOne(t *testing.T) {
	t.Parallel()

	var okCalls, badCalls int32
	var mu sync.Mutex

	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		okCalls++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		badCalls++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{
		{ID: 1, URL: bad.URL, Secret: "s"},
		{ID: 2, URL: ok.URL, Secret: "s"},
	}})

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if okCalls != 1 {
		t.Errorf("healthy subscriber called %d times, want exactly 1 -- a partial failure must not replay a success", okCalls)
	}
	if badCalls != 2 {
		t.Errorf("failing subscriber called %d times, want 2 (its full budget)", badCalls)
	}

	letters := dlq.letters(t)
	if len(letters) != 1 {
		t.Fatalf("wrote %d dead letters, want exactly 1 for the failing subscriber", len(letters))
	}
	if letters[0].SubscriptionID != 1 {
		t.Errorf("dead-lettered subscription %d, want 1 (the failing one)", letters[0].SubscriptionID)
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times, want 1 once every subscriber reached a terminal state", got)
	}
	if got := c.Stats()["delivered"]; got != 1 {
		t.Errorf("delivered count = %d, want 1", got)
	}
}

// A record that will never parse must be parked, not retried forever, or it
// blocks its partition permanently.
func TestUndecodableRecordIsParkedAndCommitted(t *testing.T) {
	t.Parallel()

	r := &fakeReader{queue: []kafka.Message{{Partition: 1, Offset: 9, Value: []byte(`{not json`)}}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{})

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	letters := dlq.letters(t)
	if len(letters) != 1 {
		t.Fatalf("wrote %d dead letters for an undecodable record, want 1", len(letters))
	}
	if !strings.Contains(letters[0].Reason, "undecodable") {
		t.Errorf("reason = %q, want it to say the record was undecodable", letters[0].Reason)
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times, want 1 -- a poison record must not block the partition", got)
	}
}

package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/history"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
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

func (f *fakeReader) Stats() kafka.ReaderStats { return kafka.ReaderStats{} }

func (f *fakeReader) Close() error { return nil }

func (f *fakeReader) commits() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.committed)
}

type blockingReader struct {
	fakeReader
	fetchStarted chan struct{}
	startOnce    sync.Once
	joined       bool
	joinReported bool
	statsCalls   int
}

func (f *blockingReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.startOnce.Do(func() { close(f.fetchStarted) })
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (f *blockingReader) Stats() kafka.ReaderStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statsCalls++
	if f.joined && !f.joinReported {
		f.joinReported = true
		return kafka.ReaderStats{Rebalances: 1}
	}
	return kafka.ReaderStats{}
}

func (f *blockingReader) markJoined() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joined = true
}

func (f *blockingReader) statsCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statsCalls
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
	messages := f.messages()
	out := make([]DeadLetter, 0, len(messages))
	for _, m := range messages {
		var dl DeadLetter
		if err := json.Unmarshal(m.Value, &dl); err != nil {
			t.Fatalf("dead letter is not decodable: %v", err)
		}
		out = append(out, dl)
	}
	return out
}

func (f *fakeDLQ) messages() []kafka.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.Message(nil), f.got...)
}

type fakeSubs struct {
	subs []subscriptions.Subscription
	fail error
}

type recordedAttempt struct {
	eventID string
	attempt history.Attempt
}

type fakeAttemptRecorder struct {
	mu   sync.Mutex
	got  []recordedAttempt
	fail error
}

func (f *fakeAttemptRecorder) RecordAttempt(ctx context.Context, eventID string, attempt history.Attempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.got = append(f.got, recordedAttempt{eventID: eventID, attempt: attempt})
	return nil
}

func (f *fakeAttemptRecorder) attempts() []recordedAttempt {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedAttempt(nil), f.got...)
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
	d := NewDeliverer(s, 2*time.Second, config.InterruptedAttemptWriteTimeout, nil)
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

func TestReadinessWaitsForGroupJoinAndAllowsZeroPartitionMember(t *testing.T) {
	t.Parallel()

	r := &blockingReader{fetchStarted: make(chan struct{})}
	c := newConsumer(t, r, &fakeDLQ{}, fakeSubs{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	<-r.fetchStarted
	if c.Ready() {
		t.Fatal("consumer became ready when fetching began but before it joined a group generation")
	}

	// The fetch remains blocked for the entire test, which is the shape of a
	// healthy group member assigned zero partitions. A successful group join is
	// enough to make that member ready.
	r.markJoined()
	deadline := time.Now().Add(time.Second)
	for !c.Ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !c.Ready() {
		t.Fatal("consumer stayed unready after joining with zero assigned partitions")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after cancellation: %v", err)
	}
	if c.Ready() {
		t.Fatal("consumer stayed ready after shutdown")
	}
}

func TestReadinessDoesNotConsumeAJoinBeforeFetchingStarts(t *testing.T) {
	t.Parallel()

	r := &blockingReader{fetchStarted: make(chan struct{})}
	r.markJoined()
	c := newConsumer(t, r, &fakeDLQ{}, fakeSubs{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.markReadyAfterGroupJoin(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// Let the watcher enter its initial check and pass at least one tick. Stats
	// must remain untouched while Run has not entered the fetch loop, because
	// kafka.Reader clears its rebalance counter when Stats reads it.
	time.Sleep(2 * groupJoinPollInterval)
	if got := r.statsCallCount(); got != 0 {
		t.Fatalf("Stats called %d times before fetching started; the join event was consumed early", got)
	}
	if c.Ready() {
		t.Fatal("consumer became ready before fetching started")
	}

	c.fetchStarted.Store(true)
	deadline := time.Now().Add(time.Second)
	for !c.Ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !c.Ready() {
		t.Fatal("consumer stayed unready after fetching started with a previously reported join")
	}
}

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
	messages := dlq.messages()
	if !bytes.Equal(messages[0].Key, rec.Key()) {
		t.Errorf("dead-letter key = %q, want tenant key %q", messages[0].Key, rec.Key())
	}
	var encoded map[string]json.RawMessage
	if err := json.Unmarshal(messages[0].Value, &encoded); err != nil {
		t.Fatalf("decode serialized dead letter: %v", err)
	}
	if _, present := encoded["source"]; present {
		t.Error("ordinary exhausted-delivery dead letter gained a source field")
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

// A subscriber can receive a webhook before Postgres records the attempt. If
// that write fails, leaving the Kafka offset uncommitted deliberately trades a
// possible duplicate on redelivery for a complete audit trail.
func TestAttemptHistoryFailureBlocksTheCommit(t *testing.T) {
	t.Parallel()

	var calls int
	var callsMu sync.Mutex
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	msg := recordMessage(t, testRecord())
	r := &fakeReader{queue: []kafka.Message{msg}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: ok.URL, Secret: "s"}}})
	recorder := &fakeAttemptRecorder{fail: errors.New("database unavailable")}
	c.deliverer.recorder = recorder

	if err := c.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil when attempt history could not be recorded")
	}
	if got := r.commits(); got != 0 {
		t.Errorf("committed %d times after attempt history failed, want 0", got)
	}
	if got := len(dlq.letters(t)); got != 0 {
		t.Errorf("wrote %d dead letters for a history failure, want 0", got)
	}

	// A new consumer fetches the uncommitted record again. The webhook is sent a
	// second time with the same event id, and this time both history and the
	// Kafka offset commit.
	recorder.mu.Lock()
	recorder.fail = nil
	recorder.mu.Unlock()
	r.mu.Lock()
	r.queue = append(r.queue, msg)
	r.mu.Unlock()
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run after database recovery: %v", err)
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times after recovery, want 1", got)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls != 2 {
		t.Errorf("subscriber received %d requests, want 2 to prove duplicate-on-retry behavior", calls)
	}
	if got := len(recorder.attempts()); got != 1 {
		t.Errorf("persisted %d attempts after recovery, want 1", got)
	}
}

// An event can reach Kafka even when WriteMessages reports a cancelled or
// ambiguous request. If its history anchor is absent, retrying cannot repair
// the record; it must be parked once so the partition can advance.
func TestMissingEventHistoryIsDeadLetteredAndCommitted(t *testing.T) {
	deliveries := func(outcome string) float64 {
		return testutil.ToFloat64(metrics.Deliveries.WithLabelValues(outcome))
	}
	deadLetters := func(reason string) float64 {
		return testutil.ToFloat64(metrics.DeadLetters.WithLabelValues(reason))
	}
	beforeDelivered := deliveries("delivered")
	beforeDeadLettered := deliveries("dead_lettered")
	beforeHistoryMissing := deadLetters("history_missing")

	var calls int
	var callsMu sync.Mutex
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()

	rec := testRecord()
	r := &fakeReader{queue: []kafka.Message{recordMessage(t, rec)}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{subs: []subscriptions.Subscription{{ID: 9, URL: ok.URL, Secret: "s"}}})
	c.deliverer.recorder = &fakeAttemptRecorder{fail: history.ErrEventNotFound}

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times, want 1 after terminally parking missing history", got)
	}
	callsMu.Lock()
	if calls != 1 {
		t.Errorf("subscriber received %d requests, want 1", calls)
	}
	callsMu.Unlock()
	letters := dlq.letters(t)
	if len(letters) != 1 {
		t.Fatalf("wrote %d dead letters, want 1", len(letters))
	}
	if letters[0].Record.ID != rec.ID || letters[0].SubscriptionID != 9 ||
		!strings.Contains(letters[0].Reason, "event history missing") {
		t.Errorf("dead letter = %+v, want event and subscription identity with missing-history reason", letters[0])
	}
	if letters[0].LastStatus != http.StatusOK {
		t.Errorf("dead letter status = %d, want %d from the completed delivery", letters[0].LastStatus, http.StatusOK)
	}
	stats := c.Stats()
	if stats["delivered"] != 1 || stats["dead_lettered"] != 0 {
		t.Errorf("stats = %v, want one delivered webhook and no failed webhook", stats)
	}
	if got := deliveries("delivered") - beforeDelivered; got != 1 {
		t.Errorf("delivered metric delta = %v, want 1", got)
	}
	if got := deliveries("dead_lettered") - beforeDeadLettered; got != 0 {
		t.Errorf("dead-lettered delivery metric delta = %v, want 0", got)
	}
	if got := deadLetters("history_missing") - beforeHistoryMissing; got != 1 {
		t.Errorf("history-missing DLQ metric delta = %v, want 1", got)
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
	writeErr := errors.New("postgres unavailable during drain")
	c.deliverer.recorder = &fakeAttemptRecorder{fail: writeErr}
	var logs bytes.Buffer
	c.log = slog.New(slog.NewJSONHandler(&logs, nil))

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
	if !strings.Contains(logs.String(), writeErr.Error()) {
		t.Errorf("shutdown log = %s, want detached history write failure", logs.String())
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

	source := kafka.Message{
		Topic:     "mlp.relay.deliveries",
		Partition: 1,
		Offset:    9,
		Time:      time.Date(2026, 9, 5, 18, 30, 0, 123000000, time.UTC),
		Key:       []byte("original-key"),
		Value:     []byte{0xff, '{', 'n', 'o', 't', ' ', 'j', 's', 'o', 'n'},
	}
	r := &fakeReader{queue: []kafka.Message{source}}
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
	dl := letters[0]
	if dl.Source == nil {
		t.Fatal("undecodable dead letter has no source identity")
	}
	if dl.Source.Topic != source.Topic || dl.Source.Partition != source.Partition || dl.Source.Offset != source.Offset ||
		!dl.Source.Timestamp.Equal(source.Time) {
		t.Errorf("source identity = %+v, want topic=%q partition=%d offset=%d timestamp=%s",
			dl.Source, source.Topic, source.Partition, source.Offset, source.Time)
	}
	if !bytes.Equal(dl.Source.Key, source.Key) || !bytes.Equal(dl.Source.RawValue, source.Value) {
		t.Errorf("source key/value = %q/%v, want %q/%v", dl.Source.Key, dl.Source.RawValue, source.Key, source.Value)
	}
	wantKeyHash := sha256.Sum256(source.Key)
	if dl.Source.KeySHA256 != fmt.Sprintf("%x", wantKeyHash) ||
		dl.Source.OriginalKeyBytes != len(source.Key) || dl.Source.KeyTruncated {
		t.Errorf("source key metadata = hash %q bytes %d truncated=%t, want %x %d false",
			dl.Source.KeySHA256, dl.Source.OriginalKeyBytes, dl.Source.KeyTruncated,
			wantKeyHash, len(source.Key))
	}
	if dl.Source.OriginalValueBytes != len(source.Value) || dl.Source.RawValueTruncated {
		t.Errorf("source byte metadata = %d truncated=%t, want %d false",
			dl.Source.OriginalValueBytes, dl.Source.RawValueTruncated, len(source.Value))
	}
	if dl.Record.TenantID != "" {
		t.Errorf("poison record inferred tenant %q from invalid bytes", dl.Record.TenantID)
	}
	messages := dlq.messages()
	wantKey := []byte("poison:mlp.relay.deliveries:1:9")
	if !bytes.Equal(messages[0].Key, wantKey) {
		t.Errorf("poison dead-letter key = %q, want deterministic key %q", messages[0].Key, wantKey)
	}
	var serialized struct {
		Source map[string]json.RawMessage `json:"source"`
	}
	if err := json.Unmarshal(messages[0].Value, &serialized); err != nil {
		t.Fatalf("decode serialized poison dead letter: %v", err)
	}
	wantSourceFields := []string{
		"topic", "partition", "offset", "timestamp", "key", "key_sha256",
		"original_key_bytes", "key_truncated", "raw_value",
		"original_value_bytes", "raw_value_truncated",
	}
	if len(serialized.Source) != len(wantSourceFields) {
		t.Errorf("serialized source fields = %v, want exactly %v", serialized.Source, wantSourceFields)
	}
	for _, field := range wantSourceFields {
		if _, present := serialized.Source[field]; !present {
			t.Errorf("serialized source is missing %q", field)
		}
	}
	if got := r.commits(); got != 1 {
		t.Errorf("committed %d times, want 1 -- a poison record must not block the partition", got)
	}
}

func TestUndecodableDLQFailureBlocksTheCommit(t *testing.T) {
	t.Parallel()

	r := &fakeReader{queue: []kafka.Message{{
		Topic: "mlp.relay.deliveries", Partition: 1, Offset: 9, Value: []byte(`{"broken":`),
	}}}
	dlq := &fakeDLQ{fail: errors.New("dlq broker unreachable")}
	c := newConsumer(t, r, dlq, fakeSubs{})

	if err := c.Run(context.Background()); err == nil {
		t.Fatal("Run returned nil when the poison dead-letter write failed")
	}
	if got := r.commits(); got != 0 {
		t.Errorf("committed %d times after a failed poison dead-letter write, want 0", got)
	}
}

func TestBoundedCopyPreservesNilAndEmpty(t *testing.T) {
	t.Parallel()

	nilCopy, nilTruncated := boundedCopy(nil, 1)
	if nilCopy != nil || nilTruncated {
		t.Errorf("nil copy = %#v truncated=%t, want nil false", nilCopy, nilTruncated)
	}
	emptyCopy, emptyTruncated := boundedCopy([]byte{}, 1)
	if emptyCopy == nil || len(emptyCopy) != 0 || emptyTruncated {
		t.Errorf("empty copy = %#v truncated=%t, want non-nil empty false", emptyCopy, emptyTruncated)
	}
}

func TestUndecodableEnvelopeIsBoundedBelowTheKafkaWriterLimit(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{'k'}, maxUndecodableKeyBytes+1)
	// time.Time's JSON decoder includes the invalid timestamp twice in its
	// error. This input caught the unbounded reason that a flat invalid byte
	// string missed.
	raw := []byte(`{"occurred_at":"` + strings.Repeat("<", 400*1024) + `"}`)
	r := &fakeReader{queue: []kafka.Message{{
		Topic: "mlp.relay.deliveries", Partition: 11, Offset: 99,
		Key: key, Value: raw,
	}}}
	dlq := &fakeDLQ{}
	c := newConsumer(t, r, dlq, fakeSubs{})

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	letters := dlq.letters(t)
	if len(letters) != 1 || letters[0].Source == nil {
		t.Fatalf("dead letters = %+v, want one poison source", letters)
	}
	source := letters[0].Source
	if len(source.Key) != maxUndecodableKeyBytes || !bytes.Equal(source.Key, key[:maxUndecodableKeyBytes]) {
		t.Errorf("bounded source key is %d bytes and does not preserve the expected prefix", len(source.Key))
	}
	wantKeyHash := sha256.Sum256(key)
	if source.KeySHA256 != fmt.Sprintf("%x", wantKeyHash) ||
		source.OriginalKeyBytes != len(key) || !source.KeyTruncated {
		t.Errorf("source key metadata = hash %q bytes %d truncated=%t, want %x %d true",
			source.KeySHA256, source.OriginalKeyBytes, source.KeyTruncated, wantKeyHash, len(key))
	}
	if len(source.RawValue) != maxUndecodableValueBytes {
		t.Errorf("raw bytes = %d, want cap %d", len(source.RawValue), maxUndecodableValueBytes)
	}
	if !bytes.Equal(source.RawValue, raw[:maxUndecodableValueBytes]) {
		t.Error("bounded raw value does not preserve the source prefix byte for byte")
	}
	if source.OriginalValueBytes != len(raw) || !source.RawValueTruncated {
		t.Errorf("source byte metadata = %d truncated=%t, want %d true",
			source.OriginalValueBytes, source.RawValueTruncated, len(raw))
	}
	if len(letters[0].Reason) > maxDecodeReasonBytes || !strings.HasSuffix(letters[0].Reason, decodeReasonSuffix) {
		t.Errorf("decode reason = %d bytes, want at most %d bytes ending %q",
			len(letters[0].Reason), maxDecodeReasonBytes, decodeReasonSuffix)
	}
	// kafka-go v0.4.51's Message.totalSize is the key, value, 22 bytes from
	// Message.size, and 1 byte for the empty header array. WriteMessages rejects
	// a record above BatchBytes before contacting Kafka.
	const kafkaGoMessageFramingBytes = 23
	message := dlq.messages()[0]
	if got := len(message.Key) + len(message.Value) + kafkaGoMessageFramingBytes; got > int(MaxDLQBatchBytes) {
		t.Errorf("encoded dead-letter message = %d bytes, want at most kafka-go BatchBytes %d",
			got, MaxDLQBatchBytes)
	}
}

// spanRecorder installs a real TracerProvider globally, which is what the
// consumer reaches through otel.Tracer. Not parallel-safe: the provider is
// process-global, so these tests do not call t.Parallel.
func spanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func spansNamed(recorder *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	out := make([]sdktrace.ReadOnlySpan, 0, 4)
	for _, span := range recorder.Ended() {
		if span.Name() == name {
			out = append(out, span)
		}
	}
	return out
}

func eventNames(span sdktrace.ReadOnlySpan) []string {
	names := make([]string, 0, len(span.Events()))
	for _, e := range span.Events() {
		names = append(names, e.Name)
	}
	return names
}

func eventAttr(span sdktrace.ReadOnlySpan, eventName, key string) string {
	for _, e := range span.Events() {
		if e.Name != eventName {
			continue
		}
		for _, attr := range e.Attributes {
			if string(attr.Key) == key {
				return attr.Value.String()
			}
		}
	}
	return ""
}

// An undecodable record is parked and the offset advances, so without an event
// and an error status the trace is indistinguishable from a clean delivery --
// and the record has no id to search by, which makes the span the only place
// the loss shows up at all.
func TestUndecodableRecordIsVisibleOnTheConsumeSpan(t *testing.T) {
	recorder := spanRecorder(t)

	r := &fakeReader{queue: []kafka.Message{{Partition: 1, Offset: 9, Value: []byte(`{not json`)}}}
	c := newConsumer(t, r, &fakeDLQ{}, fakeSubs{})
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	spans := spansNamed(recorder, "relay.consume")
	if len(spans) != 1 {
		t.Fatalf("recorded %d relay.consume spans, want 1", len(spans))
	}
	span := spans[0]
	if span.Status().Code != codes.Error {
		t.Errorf("consume span status = %v (%q), want Error -- the record was dead-lettered. "+
			"processRecord must not set codes.Ok afterwards: the SDK lets Ok override Error "+
			"and never the reverse, so an Ok here erases this.",
			span.Status().Code, span.Status().Description)
	}
	if got := eventAttr(span, "relay.dead_lettered", "relay.dead_letter.reason"); got != "undecodable" {
		t.Errorf("dead-letter reason on the span = %q, want undecodable; events = %v",
			got, eventNames(span))
	}
}

// The clean path still records the commit, and must not be marked failed.
func TestSuccessfulRecordLeavesTheConsumeSpanUnfailed(t *testing.T) {
	recorder := spanRecorder(t)

	ok := alwaysOK(t)
	defer ok.Close()
	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	c := newConsumer(t, r, &fakeDLQ{}, fakeSubs{subs: []subscriptions.Subscription{{ID: 1, URL: ok.URL, Secret: "s"}}})
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	spans := spansNamed(recorder, "relay.consume")
	if len(spans) != 1 {
		t.Fatalf("recorded %d relay.consume spans, want 1", len(spans))
	}
	if code := spans[0].Status().Code; code == codes.Error {
		t.Errorf("consume span status = Error (%q), want Unset or Ok", spans[0].Status().Description)
	}
	if !slices.Contains(eventNames(spans[0]), "kafka.offset.committed") {
		t.Errorf("consume span events = %v, want kafka.offset.committed", eventNames(spans[0]))
	}
}

// Exhausting the budget produces one span per attempt, each failed, with the
// retry delay on the span that scheduled it -- and a dead-letter event on the
// consume span.
func TestFailedDeliveryRecordsPerAttemptSpansAndADeadLetterEvent(t *testing.T) {
	recorder := spanRecorder(t)

	bad := alwaysFails(t)
	defer bad.Close()
	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	c := newConsumer(t, r, &fakeDLQ{}, fakeSubs{subs: []subscriptions.Subscription{{ID: 7, URL: bad.URL, Secret: "s"}}})
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	attempts := spansNamed(recorder, "relay.webhook.attempt")
	if len(attempts) != 2 {
		t.Fatalf("recorded %d attempt spans, want 2 (one per attempt in the 1s schedule)", len(attempts))
	}
	for i, span := range attempts {
		if span.Status().Code != codes.Error {
			t.Errorf("attempt span %d status = %v, want Error after HTTP 500", i+1, span.Status().Code)
		}
	}
	if !slices.Contains(eventNames(attempts[0]), "relay.retry.scheduled") {
		t.Errorf("first attempt span events = %v, want relay.retry.scheduled", eventNames(attempts[0]))
	}
	if slices.Contains(eventNames(attempts[1]), "relay.retry.scheduled") {
		t.Errorf("last attempt span carries relay.retry.scheduled, but its budget was spent")
	}

	consume := spansNamed(recorder, "relay.consume")
	if len(consume) != 1 {
		t.Fatalf("recorded %d relay.consume spans, want 1", len(consume))
	}
	if got := eventAttr(consume[0], "relay.dead_lettered", "relay.dead_letter.reason"); got != "delivery_exhausted" {
		t.Errorf("dead-letter reason = %q, want delivery_exhausted; events = %v",
			got, eventNames(consume[0]))
	}
}

// http.Client returns *url.Error, whose message embeds the subscriber URL with
// its path and query string. Only the password is redacted. That string must
// reach the log and the attempt-history row, never the span.
func TestTransportFailureKeepsTheSubscriberURLOffTheSpan(t *testing.T) {
	recorder := spanRecorder(t)

	// A closed listener: connecting fails, so Do returns *url.Error.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL + "/hooks/secret-path?token=s3cret"
	dead.Close()

	r := &fakeReader{queue: []kafka.Message{recordMessage(t, testRecord())}}
	c := newConsumer(t, r, &fakeDLQ{}, fakeSubs{subs: []subscriptions.Subscription{{ID: 7, URL: deadURL, Secret: "s"}}})
	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	attempts := spansNamed(recorder, "relay.webhook.attempt")
	if len(attempts) == 0 {
		t.Fatal("no attempt span was recorded")
	}
	for i, span := range attempts {
		if span.Status().Code != codes.Error {
			t.Errorf("attempt span %d status = %v, want Error", i+1, span.Status().Code)
		}
		exported := fmt.Sprintf("%v %v %v", span.Status(), span.Attributes(), span.Events())
		for _, secret := range []string{"secret-path", "s3cret"} {
			if strings.Contains(exported, secret) {
				t.Errorf("attempt span %d exports %q from the transport error: %s", i+1, secret, exported)
			}
		}
	}
}

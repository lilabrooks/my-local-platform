package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/history"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
)

type fakeProducer struct {
	mu   sync.Mutex
	got  []kafka.Message
	fail error
}

type contextProbeProducer struct {
	started chan context.Context
	release chan struct{}
}

func (p *contextProbeProducer) WriteMessages(ctx context.Context, _ ...kafka.Message) error {
	p.started <- ctx
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type fakeEventStore struct {
	mu         sync.Mutex
	events     map[string]event.Record
	byKey      map[string]string
	published  map[string]bool
	attempts   map[string][]history.Attempt
	failAccept error
	failQuery  error
}

func (f *fakeEventStore) AcceptEvent(
	ctx context.Context,
	rec event.Record,
	publish history.Publisher,
) (history.Acceptance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAccept != nil {
		return history.Acceptance{}, f.failAccept
	}
	if f.events == nil {
		f.events = make(map[string]event.Record)
	}
	if f.byKey == nil {
		f.byKey = make(map[string]string)
	}
	if f.published == nil {
		f.published = make(map[string]bool)
	}

	chosen := rec
	if rec.IdempotencyKey != "" {
		key := rec.TenantID + "\x00" + rec.IdempotencyKey
		if existingID, ok := f.byKey[key]; ok {
			chosen = f.events[existingID]
			if chosen.Type != rec.Type || !sameJSON(chosen.Data, rec.Data) {
				return history.Acceptance{}, history.ErrIdempotencyConflict
			}
		} else {
			f.byKey[key] = rec.ID
			f.events[rec.ID] = rec
		}
	} else {
		f.events[rec.ID] = rec
	}

	if f.published[chosen.ID] {
		return history.Acceptance{Record: chosen, Deduplicated: true}, nil
	}
	if err := publish(ctx, chosen); err != nil {
		return history.Acceptance{}, fmt.Errorf("%w: %w", history.ErrPublishFailed, err)
	}
	f.published[chosen.ID] = true
	return history.Acceptance{Record: chosen}, nil
}

func sameJSON(a, b json.RawMessage) bool {
	var av, bv any
	return json.Unmarshal(a, &av) == nil && json.Unmarshal(b, &bv) == nil && reflect.DeepEqual(av, bv)
}

func (f *fakeEventStore) Attempts(_ context.Context, id string) ([]history.Attempt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failQuery != nil {
		return nil, f.failQuery
	}
	if _, ok := f.events[id]; !ok {
		return nil, history.ErrEventNotFound
	}
	got := f.attempts[id]
	return append(make([]history.Attempt, 0, len(got)), got...), nil
}

func (f *fakeProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// kafka-go refuses a message carrying a Topic when the Writer already has
	// one: "Topic must not be specified for both Writer and Message". An
	// earlier fake accepted it, so this only surfaced against a real broker.
	// Enforced here so the fake cannot be more permissive than the library.
	for _, m := range msgs {
		if m.Topic != "" {
			return errors.New("kafka.(*Writer): Topic must not be specified for both Writer and Message")
		}
	}
	if f.fail != nil {
		return f.fail
	}
	f.got = append(f.got, msgs...)
	return nil
}

func (f *fakeProducer) messages() []kafka.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafka.Message(nil), f.got...)
}

func newTestServer(p Producer) http.Handler {
	return newTestServerWithEvents(p, &fakeEventStore{})
}

func TestPostEventPropagatesIncomingTraceContextToKafka(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	producer := &fakeProducer{}
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(
		`{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100}}`,
	))
	req.Header.Set("traceparent", "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01")
	req.Header.Set("tracestate", "vendor=value")
	res := httptest.NewRecorder()
	newTestServer(producer).ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", res.Code, res.Body.String())
	}

	messages := producer.messages()
	if len(messages) != 1 {
		t.Fatalf("produced %d messages, want 1", len(messages))
	}
	carrier := telemetryCarrier{headers: &messages[0].Headers}
	ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
	got := trace.SpanContextFromContext(ctx)
	if got.TraceID().String() != "0102030405060708090a0b0c0d0e0f10" {
		t.Fatalf("Kafka trace id = %s, want incoming trace id", got.TraceID())
	}
	if got.TraceState().String() != "vendor=value" {
		t.Fatalf("Kafka tracestate = %q, want vendor=value", got.TraceState())
	}
}

type telemetryCarrier struct {
	headers *[]kafka.Header
}

func (c telemetryCarrier) Get(key string) string {
	for _, header := range *c.headers {
		if strings.EqualFold(header.Key, key) {
			return string(header.Value)
		}
	}
	return ""
}

func (c telemetryCarrier) Set(string, string) {}

func (c telemetryCarrier) Keys() []string { return nil }

func newTestServerWithEvents(p Producer, events EventStore) http.Handler {
	s := New(p, events, "mlp.relay.deliveries", slog.New(slog.DiscardHandler))
	s.MarkReady(true)
	return s.Routes()
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func acceptedID(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202. body: %s", rr.Code, rr.Body)
	}
	var resp postEventResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID == "" {
		t.Fatal("accepted response has no event id")
	}
	return resp.ID
}

func TestAcceptsAndProduces(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	h := newTestServer(p)
	rawData := "{\n  \"amount\": 100, \"currency\": \"usd\"\n}"

	rr := post(t, h, `{"tenant_id":"acme","type":"invoice.paid","data":`+rawData+`}`)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202. body: %s", rr.Code, rr.Body)
	}

	var resp postEventResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !strings.HasPrefix(resp.ID, "evt_") {
		t.Errorf("response id = %q, want an evt_ prefix", resp.ID)
	}

	msgs := p.messages()
	if len(msgs) != 1 {
		t.Fatalf("produced %d messages, want 1", len(msgs))
	}
	if !bytes.Contains(msgs[0].Value, append([]byte(`"data":`), rawData...)) {
		t.Errorf("Kafka record changed data bytes: %q", msgs[0].Value)
	}
	// The partition key must be the tenant, or per-tenant ordering is lost.
	if got, want := string(msgs[0].Key), "acme"; got != want {
		t.Errorf("partition key = %q, want %q", got, want)
	}

	var rec event.Record
	if err := json.Unmarshal(msgs[0].Value, &rec); err != nil {
		t.Fatalf("decode produced record: %v", err)
	}
	if rec.ID != resp.ID {
		t.Errorf("record id %q does not match the id returned to the caller %q", rec.ID, resp.ID)
	}
	if rec.OccurredAt.IsZero() {
		t.Error("record has no occurred_at")
	}
}

func TestIdenticalIdempotentRequestsReturnOneEvent(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	h := newTestServer(p)
	body := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100},"idempotency_key":"payment-42"}`

	first := acceptedID(t, post(t, h, body))
	second := acceptedID(t, post(t, h, body))
	if second != first {
		t.Errorf("second event id = %q, want original %q", second, first)
	}
	if got := len(p.messages()); got != 1 {
		t.Errorf("produced %d Kafka messages, want 1", got)
	}
}

func TestConcurrentIdempotentRequestsPublishOnce(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	h := newTestServer(p)
	body := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100},"idempotency_key":"concurrent-42"}`
	responses := make([]*httptest.ResponseRecorder, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range responses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			responses[i] = post(t, h, body)
		}()
	}
	close(start)
	wg.Wait()

	first := acceptedID(t, responses[0])
	second := acceptedID(t, responses[1])
	if second != first {
		t.Errorf("concurrent ids = %q and %q, want the same id", first, second)
	}
	if got := len(p.messages()); got != 1 {
		t.Errorf("produced %d Kafka messages, want 1", got)
	}
}

func TestIdempotencyConflictProducesNothing(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	h := newTestServer(p)
	first := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100},"idempotency_key":"payment-42"}`
	conflict := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":200},"idempotency_key":"payment-42"}`

	_ = acceptedID(t, post(t, h, first))
	rr := post(t, h, conflict)
	if rr.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409. body: %s", rr.Code, rr.Body)
	}
	if got := len(p.messages()); got != 1 {
		t.Errorf("produced %d Kafka messages after conflict, want 1 total", got)
	}
}

func TestIdempotencyKeyIsScopedToTenant(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	h := newTestServer(p)
	acme := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100},"idempotency_key":"payment-42"}`
	globex := `{"tenant_id":"globex","type":"invoice.paid","data":{"amount":100},"idempotency_key":"payment-42"}`

	first := acceptedID(t, post(t, h, acme))
	second := acceptedID(t, post(t, h, globex))
	if second == first {
		t.Errorf("different tenants received the same event id %q", first)
	}
	if got := len(p.messages()); got != 2 {
		t.Errorf("produced %d Kafka messages, want 2", got)
	}
}

func TestBlankIdempotencyKeysCreateNewEvents(t *testing.T) {
	t.Parallel()

	for name, field := range map[string]string{
		"missing":    "",
		"empty":      `,"idempotency_key":""`,
		"whitespace": `,"idempotency_key":"  "`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := &fakeProducer{}
			h := newTestServer(p)
			body := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100}` + field + `}`
			first := acceptedID(t, post(t, h, body))
			second := acceptedID(t, post(t, h, body))
			if second == first {
				t.Errorf("blank key reused event id %q", first)
			}
			if got := len(p.messages()); got != 2 {
				t.Errorf("produced %d Kafka messages, want 2", got)
			}
		})
	}
}

// The trailing-content check rejects a second JSON value, not trailing bytes
// of any kind. A body ending in a newline is what curl -d @file and most HTTP
// clients send, and rejecting it would turn a correctness fix into an outage.
func TestTrailingWhitespaceIsAccepted(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"newline":     "{\"tenant_id\":\"acme\",\"type\":\"a\",\"data\":{}}\n",
		"crlf":        "{\"tenant_id\":\"acme\",\"type\":\"a\",\"data\":{}}\r\n",
		"spaces":      `{"tenant_id":"acme","type":"a","data":{}}   `,
		"mixed space": "{\"tenant_id\":\"acme\",\"type\":\"a\",\"data\":{}} \t\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := &fakeProducer{}
			rr := post(t, newTestServer(p), body)
			if rr.Code != http.StatusAccepted {
				t.Errorf("status = %d, want 202. body: %s", rr.Code, rr.Body)
			}
			if n := len(p.messages()); n != 1 {
				t.Errorf("produced %d messages, want 1", n)
			}
		})
	}
}

// The one outcome that must never happen is success when Kafka reports failure.
// The event row stays because the write may have reached Kafka before its
// caller's context expired.
func TestBrokerFailureIsNotSuccess(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{fail: errors.New("broker unreachable")}
	events := &fakeEventStore{}
	h := newTestServerWithEvents(p, events)
	body := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100},"idempotency_key":"payment-42"}`

	rr := post(t, h, body)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "evt_") {
		t.Errorf("a rejected event was given an id the caller might record: %s", rr.Body)
	}
	events.mu.Lock()
	if len(events.events) != 1 {
		t.Errorf("broker failure retained %d event rows, want 1 for an ambiguous Kafka write", len(events.events))
	}
	var retainedID string
	for retainedID = range events.events {
	}
	if events.published[retainedID] {
		t.Error("broker failure marked the retained idempotency claim as published")
	}
	events.mu.Unlock()

	p.mu.Lock()
	p.fail = nil
	p.mu.Unlock()
	recoveredID := acceptedID(t, post(t, h, body))
	if recoveredID != retainedID {
		t.Errorf("recovery event id = %q, want retained id %q", recoveredID, retainedID)
	}
	deduplicatedID := acceptedID(t, post(t, h, body))
	if deduplicatedID != retainedID {
		t.Errorf("deduplicated event id = %q, want retained id %q", deduplicatedID, retainedID)
	}
	if got := len(p.messages()); got != 1 {
		t.Errorf("successful recovery and repeat produced %d messages, want 1", got)
	}
}

func TestAcceptanceSurvivesClientCancellation(t *testing.T) {
	p := &contextProbeProducer{
		started: make(chan context.Context, 1),
		release: make(chan struct{}),
	}
	h := newTestServer(p)
	requestContext, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(
		`{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100},"idempotency_key":"disconnect-42"}`,
	)).WithContext(requestContext)
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(rr, req)
		close(done)
	}()

	var acceptanceContext context.Context
	select {
	case acceptanceContext = <-p.started:
	case <-time.After(time.Second):
		t.Fatal("producer was not called")
	}
	deadline, ok := acceptanceContext.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > acceptanceTimeout {
		t.Fatalf("acceptance context deadline = %v, want a live deadline within %s", deadline, acceptanceTimeout)
	}

	cancelRequest()
	if err := acceptanceContext.Err(); err != nil {
		t.Fatalf("client cancellation reached in-flight acceptance: %v", err)
	}
	close(p.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not finish after Kafka acknowledgement")
	}
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 after detached acceptance completed: %s", rr.Code, rr.Body)
	}
}

func TestEventHistoryFailureIsNotSuccess(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	events := &fakeEventStore{failAccept: errors.New("database unavailable")}
	rr := post(t, newTestServerWithEvents(p, events),
		`{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100}}`)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if got := len(p.messages()); got != 0 {
		t.Errorf("produced %d messages after event persistence failed, want 0", got)
	}
}

func TestGetAttemptsDistinguishesUnknownAndEmptyHistory(t *testing.T) {
	t.Parallel()

	events := &fakeEventStore{}
	h := newTestServerWithEvents(&fakeProducer{}, events)
	accepted := post(t, h, `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100}}`)
	var created postEventResponse
	if err := json.Unmarshal(accepted.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode accepted event: %v", err)
	}

	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr
	}

	empty := get("/v1/events/" + created.ID + "/attempts")
	if empty.Code != http.StatusOK {
		t.Fatalf("known event status = %d, want 200: %s", empty.Code, empty.Body)
	}
	var got attemptsResponse
	if err := json.Unmarshal(empty.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode empty history: %v", err)
	}
	if got.EventID != created.ID || got.Attempts == nil || len(got.Attempts) != 0 {
		t.Errorf("empty history = %+v, want event %s and [] attempts", got, created.ID)
	}

	status := http.StatusOK
	now := time.Now().UTC()
	events.mu.Lock()
	events.attempts = map[string][]history.Attempt{created.ID: {{
		SubscriptionID:  7,
		SubscriptionURL: "http://sink:8081/hooks/ok",
		AttemptNumber:   1,
		StartedAt:       now,
		FinishedAt:      now.Add(time.Millisecond),
		HTTPStatus:      &status,
		Outcome:         history.OutcomeDelivered,
	}}}
	events.mu.Unlock()

	withAttempt := get("/v1/events/" + created.ID + "/attempts")
	if err := json.Unmarshal(withAttempt.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode attempt history: %v", err)
	}
	if len(got.Attempts) != 1 || got.Attempts[0].SubscriptionID != 7 ||
		got.Attempts[0].Outcome != history.OutcomeDelivered {
		t.Errorf("attempt history = %+v, want delivered subscription 7", got.Attempts)
	}

	unknown := get("/v1/events/evt_unknown/attempts")
	if unknown.Code != http.StatusNotFound {
		t.Errorf("unknown event status = %d, want 404", unknown.Code)
	}

	events.mu.Lock()
	events.failQuery = errors.New("database unavailable")
	events.mu.Unlock()
	unavailable := get("/v1/events/" + created.ID + "/attempts")
	if unavailable.Code != http.StatusServiceUnavailable {
		t.Errorf("database failure status = %d, want 503", unavailable.Code)
	}
}

func TestRejectsBadRequests(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		body string
		want int
	}{
		"malformed json": {`{"tenant_id":`, http.StatusBadRequest},
		"empty body":     {``, http.StatusBadRequest},
		"no tenant":      {`{"type":"a","data":{}}`, http.StatusBadRequest},
		"no type":        {`{"tenant_id":"acme","data":{}}`, http.StatusBadRequest},
		"no data":        {`{"tenant_id":"acme","type":"a"}`, http.StatusBadRequest},
		"scalar data":    {`{"tenant_id":"acme","type":"a","data":7}`, http.StatusBadRequest},
		"unknown field":  {`{"tenant_id":"acme","type":"a","data":{},"typo":1}`, http.StatusBadRequest},
		// A body is one JSON text. Decode stops at the end of the first value
		// without erroring, so these were accepted with a 202 while everything
		// after the first object was discarded.
		"trailing object": {
			`{"tenant_id":"acme","type":"a","data":{}}{"tenant_id":"evil","type":"b","data":{}}`,
			http.StatusBadRequest,
		},
		"trailing garbage": {
			`{"tenant_id":"acme","type":"a","data":{}}not-json`,
			http.StatusBadRequest,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			p := &fakeProducer{}
			rr := post(t, newTestServer(p), tc.body)
			if rr.Code != tc.want {
				t.Errorf("status = %d, want %d. body: %s", rr.Code, tc.want, rr.Body)
			}
			if n := len(p.messages()); n != 0 {
				t.Errorf("a rejected request produced %d messages, want 0", n)
			}
		})
	}
}

// An oversized body must be reported as too large, not as truncated JSON --
// otherwise the caller goes looking for a syntax error that is not there.
func TestOversizedBodyIsNotAParseError(t *testing.T) {
	t.Parallel()

	huge := `{"tenant_id":"acme","type":"a","data":{"x":"` + strings.Repeat("y", MaxBodyBytes) + `"}}`
	rr := post(t, newTestServer(&fakeProducer{}), huge)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413. body: %s", rr.Code, rr.Body)
	}
}

func TestReadinessGatesTraffic(t *testing.T) {
	t.Parallel()

	s := New(&fakeProducer{}, &fakeEventStore{}, "t", slog.New(slog.DiscardHandler))
	h := s.Routes()

	// Not ready until told: a pod that reports ready before its broker
	// connection exists gets traffic it cannot serve.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz before MarkReady = %d, want 503", rr.Code)
	}

	// Liveness is independent of readiness, or a draining pod gets restarted
	// instead of drained.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Errorf("healthz before MarkReady = %d, want 200", rr.Code)
	}

	s.MarkReady(true)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Errorf("readyz after MarkReady = %d, want 200", rr.Code)
	}

	s.MarkReady(false)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz after draining = %d, want 503", rr.Code)
	}
}

func TestGetOnEventsIsRejected(t *testing.T) {
	t.Parallel()

	rr := httptest.NewRecorder()
	newTestServer(&fakeProducer{}).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/events", nil))
	if rr.Code == http.StatusOK {
		t.Errorf("GET /v1/events returned 200, want a rejection")
	}
	_, _ = io.Copy(io.Discard, rr.Body)
}

func TestMetricsEndpointCountsOutcomes(t *testing.T) {
	// The counters are package-level in internal/metrics, so this asserts on
	// the delta rather than an absolute value: other tests in this binary
	// increment the same series.
	before := func(outcome string) float64 {
		return testutil.ToFloat64(metrics.IngestEvents.WithLabelValues(outcome))
	}
	acceptedBefore, invalidBefore := before("accepted"), before("invalid")

	h := newTestServer(&fakeProducer{})

	if got := post(t, h, `{"tenant_id":"acme","type":"order.created","data":{"n":1}}`).Code; got != http.StatusAccepted {
		t.Fatalf("POST /v1/events = %d, want 202", got)
	}
	// Missing tenant_id: rejected by Record.Validate, so it lands under
	// "invalid" and not under "malformed", which is reserved for JSON that
	// would not decode at all.
	if got := post(t, h, `{"type":"order.created","data":{"n":1}}`).Code; got != http.StatusBadRequest {
		t.Fatalf("POST with no tenant_id = %d, want 400", got)
	}

	if got := before("accepted") - acceptedBefore; got != 1 {
		t.Errorf("accepted counter moved by %v, want 1", got)
	}
	if got := before("invalid") - invalidBefore; got != 1 {
		t.Errorf("invalid counter moved by %v, want 1", got)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	// Routed on the same mux as POST /v1/events rather than on a second
	// listener, because the Deployment exposes one port and Prometheus has to
	// reach this through the same Service as everything else.
	if !strings.Contains(rec.Body.String(), "relay_ingest_events_total") {
		t.Error("GET /metrics did not expose relay_ingest_events_total")
	}
}

// spanRecorder installs a real TracerProvider globally and returns the spans a
// test produced. The global provider is what postEvent reaches through
// otel.Tracer, so this is the only way to see what relay actually exports.
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

func spanNamed(t *testing.T, recorder *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var found sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == name {
			found = span
		}
	}
	if found == nil {
		t.Fatalf("no %s span was recorded", name)
	}
	return found
}

func spanAttr(span sdktrace.ReadOnlySpan, key string) string {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.String()
		}
	}
	return ""
}

func postJSON(handler http.Handler, body string, header http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	for key, values := range header {
		req.Header[key] = values
	}
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	return res
}

// A deduplicated request is handed the FIRST request's id, and that is the id
// the Kafka record and every delivery span carry. Tagging its ingest span with
// the id generated-and-discarded on this request would put an id in the trace
// that names no event, and the runbook's per-event query would miss the span
// for the request the caller actually made.
func TestPostEventSpanCarriesTheAcceptedEventIDNotTheDiscardedCandidate(t *testing.T) {
	recorder := spanRecorder(t)
	handler := newTestServerWithEvents(&fakeProducer{}, &fakeEventStore{})
	body := `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100},"idempotency_key":"k1"}`

	first := postJSON(handler, body, nil)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202: %s", first.Code, first.Body.String())
	}
	var accepted postEventResponse
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil {
		t.Fatalf("decode first response: %v", err)
	}

	second := postJSON(handler, body, nil)
	if second.Code != http.StatusAccepted {
		t.Fatalf("second status = %d, want 202: %s", second.Code, second.Body.String())
	}
	var deduplicated postEventResponse
	if err := json.Unmarshal(second.Body.Bytes(), &deduplicated); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if deduplicated.ID != accepted.ID {
		t.Fatalf("second request returned %s, want the first id %s", deduplicated.ID, accepted.ID)
	}

	ingestSpans := make([]sdktrace.ReadOnlySpan, 0, 2)
	for _, span := range recorder.Ended() {
		if span.Name() == "relay.ingest" {
			ingestSpans = append(ingestSpans, span)
		}
	}
	if len(ingestSpans) != 2 {
		t.Fatalf("recorded %d relay.ingest spans, want 2", len(ingestSpans))
	}
	for i, span := range ingestSpans {
		if got := spanAttr(span, "relay.event.id"); got != accepted.ID {
			t.Errorf("relay.ingest span %d has relay.event.id %q, want the accepted id %q",
				i+1, got, accepted.ID)
		}
	}
	if got := spanAttr(ingestSpans[1], "relay.event.deduplicated"); got != "true" {
		t.Errorf("deduplicated span reports relay.event.deduplicated=%q, want true", got)
	}
}

// The conflict error names the caller's idempotency key, which can be an order
// id or a customer reference. It belongs in the log and the 409 body, not in a
// span that ships to a trace backend.
func TestPostEventConflictSpanDoesNotCarryTheIdempotencyKey(t *testing.T) {
	recorder := spanRecorder(t)
	handler := newTestServerWithEvents(&fakeProducer{}, &fakeEventStore{})
	const key = "customer-42-order-99"

	first := postJSON(handler, `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":1},"idempotency_key":"`+key+`"}`, nil)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want 202: %s", first.Code, first.Body.String())
	}
	conflict := postJSON(handler, `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":2},"idempotency_key":"`+key+`"}`, nil)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want 409: %s", conflict.Code, conflict.Body.String())
	}

	span := spanNamed(t, recorder, "relay.ingest")
	if span.Status().Code != codes.Error {
		t.Errorf("conflict span status = %v, want Error", span.Status().Code)
	}
	if len(span.Events()) != 0 {
		t.Errorf("conflict span carries %d events, want none", len(span.Events()))
	}
	exported := fmt.Sprintf("%v %v %v", span.Status(), span.Attributes(), span.Events())
	if strings.Contains(exported, key) {
		t.Errorf("span exports the idempotency key %q: %s", key, exported)
	}
}

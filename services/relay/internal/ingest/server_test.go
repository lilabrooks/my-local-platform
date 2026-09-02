package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/history"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
)

type fakeProducer struct {
	mu   sync.Mutex
	got  []kafka.Message
	fail error
}

type fakeEventStore struct {
	mu         sync.Mutex
	events     map[string]event.Record
	attempts   map[string][]history.Attempt
	failCreate error
	failQuery  error
}

func (f *fakeEventStore) CreateEvent(_ context.Context, rec event.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCreate != nil {
		return f.failCreate
	}
	if f.events == nil {
		f.events = make(map[string]event.Record)
	}
	f.events[rec.ID] = rec
	return nil
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

func TestAcceptsAndProduces(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	h := newTestServer(p)

	rr := post(t, h, `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100}}`)
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

	rr := post(t, h, `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100}}`)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "evt_") {
		t.Errorf("a rejected event was given an id the caller might record: %s", rr.Body)
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	if len(events.events) != 1 {
		t.Errorf("broker failure retained %d event rows, want 1 for an ambiguous Kafka write", len(events.events))
	}
}

func TestEventHistoryFailureIsNotSuccess(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	events := &fakeEventStore{failCreate: errors.New("database unavailable")}
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

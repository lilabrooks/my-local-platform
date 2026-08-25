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

	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
)

type fakeProducer struct {
	mu   sync.Mutex
	got  []kafka.Message
	fail error
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
	s := New(p, "mlp.relay.deliveries", slog.New(slog.DiscardHandler))
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

// The one outcome that must never happen: a success for an event that was not
// written. An unreachable broker is a 503, and nothing is buffered.
func TestBrokerFailureIsNotSuccess(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{fail: errors.New("broker unreachable")}
	h := newTestServer(p)

	rr := post(t, h, `{"tenant_id":"acme","type":"invoice.paid","data":{"amount":100}}`)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "evt_") {
		t.Errorf("a rejected event was given an id the caller might record: %s", rr.Body)
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

	s := New(&fakeProducer{}, "t", slog.New(slog.DiscardHandler))
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

package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testSecret = "test-signing-secret"

func newSink() *sink { return &sink{secret: []byte(testSecret)} }

// signedHeaders builds what relay is expected to send. Written here from the
// specification rather than imported from relay, on purpose -- see the package
// comment.
func signedHeaders(id string, ts time.Time, body []byte, secret string) http.Header {
	unix := strconv.FormatInt(ts.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(id + "." + unix + "."))
	mac.Write(body)

	h := http.Header{}
	h.Set("webhook-id", id)
	h.Set("webhook-timestamp", unix)
	h.Set("webhook-signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	return h
}

func TestVerifyAcceptsAGoodSignature(t *testing.T) {
	t.Parallel()

	body := []byte(`{"type":"invoice.paid","data":{"amount":100}}`)
	now := time.Now()
	h := signedHeaders("evt_1", now, body, testSecret)

	if err := newSink().verify(h, body, now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
}

func TestVerifyRejects(t *testing.T) {
	t.Parallel()

	body := []byte(`{"type":"invoice.paid","data":{}}`)
	now := time.Now()

	cases := map[string]func() (http.Header, []byte){
		"wrong secret": func() (http.Header, []byte) {
			return signedHeaders("evt_1", now, body, "not-the-secret"), body
		},
		"tampered body": func() (http.Header, []byte) {
			return signedHeaders("evt_1", now, body, testSecret), []byte(`{"type":"invoice.paid","data":{"amount":999999}}`)
		},
		"tampered id": func() (http.Header, []byte) {
			h := signedHeaders("evt_1", now, body, testSecret)
			h.Set("webhook-id", "evt_2")
			return h, body
		},
		"tampered timestamp": func() (http.Header, []byte) {
			h := signedHeaders("evt_1", now, body, testSecret)
			h.Set("webhook-timestamp", strconv.FormatInt(now.Add(time.Minute).Unix(), 10))
			return h, body
		},
		"no headers at all": func() (http.Header, []byte) {
			return http.Header{}, body
		},
		"missing signature": func() (http.Header, []byte) {
			h := signedHeaders("evt_1", now, body, testSecret)
			h.Del("webhook-signature")
			return h, body
		},
		"unknown signature version": func() (http.Header, []byte) {
			h := signedHeaders("evt_1", now, body, testSecret)
			h.Set("webhook-signature", "v2,"+h.Get("webhook-signature")[3:])
			return h, body
		},
		"signature without version prefix": func() (http.Header, []byte) {
			h := signedHeaders("evt_1", now, body, testSecret)
			h.Set("webhook-signature", "abcdef")
			return h, body
		},
		"non-numeric timestamp": func() (http.Header, []byte) {
			h := signedHeaders("evt_1", now, body, testSecret)
			h.Set("webhook-timestamp", "yesterday")
			return h, body
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h, b := build()
			if err := newSink().verify(h, b, now); err == nil {
				t.Error("verify() = nil, want a rejection")
			}
		})
	}
}

// A correctly signed delivery from outside the tolerance window is a replay.
func TestVerifyRejectsReplays(t *testing.T) {
	t.Parallel()

	body := []byte(`{"type":"a","data":{}}`)
	now := time.Now()

	old := now.Add(-toleranceWindow - time.Minute)
	if err := newSink().verify(signedHeaders("evt_1", old, body, testSecret), body, now); err == nil {
		t.Error("a delivery older than the tolerance window was accepted")
	}

	future := now.Add(toleranceWindow + time.Minute)
	if err := newSink().verify(signedHeaders("evt_1", future, body, testSecret), body, now); err == nil {
		t.Error("a delivery from the future was accepted")
	}

	// Just inside the window is fine -- clocks drift.
	recent := now.Add(-toleranceWindow + time.Minute)
	if err := newSink().verify(signedHeaders("evt_1", recent, body, testSecret), body, now); err != nil {
		t.Errorf("a delivery inside the tolerance window was rejected: %v", err)
	}
}

// Standard Webhooks allows several signatures in one header; any valid one
// counts, so a sender rotating secrets is not rejected.
func TestVerifyAcceptsOneOfSeveralSignatures(t *testing.T) {
	t.Parallel()

	body := []byte(`{"type":"a","data":{}}`)
	now := time.Now()
	good := signedHeaders("evt_1", now, body, testSecret).Get("webhook-signature")
	stale := signedHeaders("evt_1", now, body, "an-old-secret").Get("webhook-signature")

	h := signedHeaders("evt_1", now, body, testSecret)
	h.Set("webhook-signature", stale+" "+good)
	if err := newSink().verify(h, body, now); err != nil {
		t.Errorf("a header carrying a stale and a valid signature was rejected: %v", err)
	}

	h.Set("webhook-signature", stale)
	if err := newSink().verify(h, body, now); err == nil {
		t.Error("a header carrying only a stale signature was accepted")
	}
}

func TestRollIsDeterministicAtTheBounds(t *testing.T) {
	t.Parallel()

	for i := 0; i < 100; i++ {
		if roll(0) {
			t.Fatal("roll(0) returned true; a zero rate must never fail a delivery")
		}
		if !roll(1) {
			t.Fatal("roll(1) returned false; a rate of 1 must always fail a delivery")
		}
	}
}

func TestFailRateRoundTrip(t *testing.T) {
	t.Parallel()

	s := newSink()
	for _, want := range []float64{0, 0.25, 0.5, 1} {
		s.setFailRate(want)
		if got := s.failRateValue(); got != want {
			t.Errorf("failRateValue() = %v, want %v", got, want)
		}
	}
}

func TestLatencyRoundTrip(t *testing.T) {
	t.Parallel()

	s := newSink()
	s.setLatency(250 * time.Millisecond)
	if got, want := s.latency(), 250*time.Millisecond; got != want {
		t.Errorf("latency() = %s, want %s", got, want)
	}
}

// deliver posts one correctly signed delivery through a handler and returns the
// status the sink answered with.
func deliver(t *testing.T, s *sink, path string, failRate float64) int {
	t.Helper()

	body := []byte(`{"type":"invoice.paid","data":{"amount":100}}`)
	now := time.Now()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	for k, v := range signedHeaders("evt_1", now, body, testSecret) {
		req.Header[k] = v
	}

	rec := httptest.NewRecorder()
	s.handle(failRate)(rec, req)
	return rec.Code
}

func scrape(t *testing.T, s *sink) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.metrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus text format", ct)
	}
	return rec.Body.String()
}

func TestMetricsCountsDeliveriesByPathAndStatus(t *testing.T) {
	s := newSink()

	if got := deliver(t, s, "/hooks/ok", 0); got != http.StatusOK {
		t.Fatalf("delivery to /hooks/ok = %d, want 200", got)
	}
	if got := deliver(t, s, "/hooks/ok", 0); got != http.StatusOK {
		t.Fatalf("second delivery to /hooks/ok = %d, want 200", got)
	}
	// failRate 1 is the deterministic always-fail the smoke check relies on.
	if got := deliver(t, s, "/hooks/flaky", 1); got != http.StatusInternalServerError {
		t.Fatalf("delivery to /hooks/flaky = %d, want 500", got)
	}

	body := scrape(t, s)
	for _, want := range []string{
		`sink_deliveries_total{path="/hooks/flaky",status="500"} 1`,
		`sink_deliveries_total{path="/hooks/ok",status="200"} 2`,
		"sink_received_total 3",
		"sink_received_retained 3",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %q\ngot:\n%s", want, body)
		}
	}
}

func TestReceivedTotalSurvivesClearingTheHistory(t *testing.T) {
	s := newSink()
	deliver(t, s, "/hooks/ok", 0)
	deliver(t, s, "/hooks/ok", 0)

	// The smoke check clears the history between runs. A counter that reset
	// with it would make rate() report a spike every time, so the two numbers
	// are deliberately different things.
	rec := httptest.NewRecorder()
	s.deleteReceived(rec, httptest.NewRequest(http.MethodDelete, "/received", nil))

	body := scrape(t, s)
	if !strings.Contains(body, "sink_received_total 2") {
		t.Errorf("sink_received_total went backwards after DELETE /received\ngot:\n%s", body)
	}
	if !strings.Contains(body, "sink_received_retained 0") {
		t.Errorf("sink_received_retained did not drop after DELETE /received\ngot:\n%s", body)
	}
}

func TestMetricsExportsTheRuntimeKnobs(t *testing.T) {
	s := newSink()
	s.setLatency(2000 * time.Millisecond)
	s.setFailRate(0.25)

	// These two are what POST /control changes mid-demo. Exporting them puts
	// the cause on the same panel as the lag it produces.
	body := scrape(t, s)
	for _, want := range []string{"sink_latency_ms 2000", "sink_fail_rate 0.25"} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape is missing %q\ngot:\n%s", want, body)
		}
	}
}

func TestMetricsRejectsUnverifiedDeliveries(t *testing.T) {
	s := newSink()

	// Wrong secret: the sink answers 401 and the delivery must not appear in
	// sink_deliveries_total, which counts what was actually accepted.
	body := []byte(`{"type":"invoice.paid"}`)
	req := httptest.NewRequest(http.MethodPost, "/hooks/ok", bytes.NewReader(body))
	for k, v := range signedHeaders("evt_1", time.Now(), body, "not-the-secret") {
		req.Header[k] = v
	}
	rec := httptest.NewRecorder()
	s.handle(0)(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forged delivery = %d, want 401", rec.Code)
	}

	out := scrape(t, s)
	if !strings.Contains(out, "sink_rejected_total 1") {
		t.Errorf("scrape is missing sink_rejected_total 1\ngot:\n%s", out)
	}
	if !strings.Contains(out, "sink_received_total 0") {
		t.Errorf("a rejected delivery was counted as received\ngot:\n%s", out)
	}
}

// TestRetentionStopsGrowing is the point of issue #23. A sink that ran for a
// long time used to hold every delivery it had ever accepted.
func TestRetentionStopsGrowing(t *testing.T) {
	s := newSink()
	s.retain = 10

	for i := range 200 {
		s.record(delivery{WebhookID: fmt.Sprintf("evt_%03d", i), Path: "/hooks/ok"}, 200)
	}

	if got := s.count(); got != 10 {
		t.Errorf("retained %d deliveries, want the bound of 10", got)
	}
	if got := len(s.received); got != 10 {
		t.Errorf("backing slice grew to %d, want 10 -- the ring is not reusing entries", got)
	}
	// The counter must NOT be bounded: eviction is not un-receiving.
	if got := s.receivedTotal.Load(); got != 200 {
		t.Errorf("sink_received_total = %d, want 200 -- eviction made the counter lie", got)
	}
}

// TestRetentionKeepsTheNewest checks which end is dropped. Keeping the oldest
// would be worse than not bounding at all: the smoke check polls for the
// delivery it just triggered, which is always the newest.
func TestRetentionKeepsTheNewest(t *testing.T) {
	s := newSink()
	s.retain = 3

	for i := range 10 {
		s.record(delivery{WebhookID: fmt.Sprintf("evt_%d", i)}, 200)
	}

	got := s.snapshot(0)
	want := []string{"evt_7", "evt_8", "evt_9"}
	if len(got) != len(want) {
		t.Fatalf("snapshot has %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].WebhookID != want[i] {
			t.Errorf("snapshot[%d] = %s, want %s (oldest first, newest last)",
				i, got[i].WebhookID, want[i])
		}
	}
}

// TestSnapshotLimitReturnsTheTail covers the other half of #23: GET /received
// serialised the entire history on every call, and the relay smoke check polls
// it every 250ms.
func TestSnapshotLimitReturnsTheTail(t *testing.T) {
	s := newSink()
	s.retain = 100
	for i := range 50 {
		s.record(delivery{WebhookID: fmt.Sprintf("evt_%d", i)}, 200)
	}

	got := s.snapshot(5)
	if len(got) != 5 {
		t.Fatalf("limit=5 returned %d entries", len(got))
	}
	if got[4].WebhookID != "evt_49" || got[0].WebhookID != "evt_45" {
		t.Errorf("limit returned %s..%s, want evt_45..evt_49 -- the tail, not the head",
			got[0].WebhookID, got[4].WebhookID)
	}
	// A limit larger than what is held is not an error, and must not pad.
	if got := s.snapshot(500); len(got) != 50 {
		t.Errorf("limit above the retained count returned %d, want 50", len(got))
	}
}

func TestGetReceivedReportsRetainedAndTotalSeparately(t *testing.T) {
	s := newSink()
	s.retain = 5
	for i := range 20 {
		s.record(delivery{WebhookID: fmt.Sprintf("evt_%d", i)}, 200)
	}

	rec := httptest.NewRecorder()
	s.getReceived(rec, httptest.NewRequest(http.MethodGet, "/received?limit=2", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /received?limit=2 = %d", rec.Code)
	}
	var body struct {
		Count      int   `json:"count"`
		Retained   int   `json:"retained"`
		Total      int64 `json:"total"`
		Deliveries []struct {
			WebhookID string `json:"webhook_id"`
		} `json:"deliveries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Three different numbers, and conflating any two would mislead: 2 asked
	// for, 5 held, 20 ever seen.
	if body.Count != 2 || body.Retained != 5 || body.Total != 20 {
		t.Errorf("count=%d retained=%d total=%d, want 2/5/20",
			body.Count, body.Retained, body.Total)
	}
	if len(body.Deliveries) != 2 || body.Deliveries[1].WebhookID != "evt_19" {
		t.Errorf("deliveries did not end at the newest entry: %+v", body.Deliveries)
	}
}

func TestGetReceivedRejectsABadLimit(t *testing.T) {
	s := newSink()
	for _, bad := range []string{"-1", "abc", "1.5"} {
		rec := httptest.NewRecorder()
		s.getReceived(rec, httptest.NewRequest(http.MethodGet, "/received?limit="+bad, nil))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("limit=%s returned %d, want 400", bad, rec.Code)
		}
	}
}

// TestUnboundedRetentionIsStillPossible proves the bound is what stops the
// growth, rather than something else in the rewrite doing it by accident.
func TestUnboundedRetentionIsStillPossible(t *testing.T) {
	s := newSink()
	s.retain = -1
	for i := range 500 {
		s.record(delivery{WebhookID: fmt.Sprintf("evt_%d", i)}, 200)
	}
	if got := s.count(); got != 500 {
		t.Errorf("unbounded sink retained %d of 500 -- the ring bounded it anyway", got)
	}
}

package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strconv"
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

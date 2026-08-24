package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newServer(t *testing.T) *server {
	t.Helper()
	s := &server{started: time.Now()}
	s.ready.Store(true)
	return s
}

func TestHealthzAlwaysOK(t *testing.T) {
	s := newServer(t)
	rec := httptest.NewRecorder()

	s.healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %q, want ok", body["status"])
	}
}

// Readiness must fail once the process is draining. This is the behaviour that
// makes a rolling update safe: Kubernetes pulls the endpoint out of the Service
// before the container stops accepting connections.
func TestReadyzReflectsDraining(t *testing.T) {
	s := newServer(t)

	rec := httptest.NewRecorder()
	s.readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready server returned %d, want 200", rec.Code)
	}

	s.ready.Store(false)

	rec = httptest.NewRecorder()
	s.readyz(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("draining server returned %d, want 503", rec.Code)
	}
}

// Liveness must NOT follow readiness. If it did, Kubernetes would kill the pod
// mid-drain instead of letting it finish in-flight requests.
func TestLivenessIndependentOfReadiness(t *testing.T) {
	s := newServer(t)
	s.ready.Store(false)

	rec := httptest.NewRecorder()
	s.healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("healthz returned %d while draining, want 200 -- a draining pod is alive, not dead", rec.Code)
	}
}

func TestMetricsIsPrometheusText(t *testing.T) {
	s := newServer(t)
	s.requests.Store(7)

	rec := httptest.NewRecorder()
	s.metrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", got)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"# HELP echo_requests_total",
		"# TYPE echo_requests_total counter",
		"echo_requests_total 7",
		"# TYPE echo_uptime_seconds gauge",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q\ngot:\n%s", want, body)
		}
	}
}

// The counter is what /metrics reports, so it has to move with real traffic.
func TestCountMiddlewareCountsEveryRequest(t *testing.T) {
	s := newServer(t)
	handler := s.count(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for range 3 {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	}

	if got := s.requests.Load(); got != 3 {
		t.Errorf("requests = %d, want 3", got)
	}
}

func TestRootReportsIdentity(t *testing.T) {
	s := newServer(t)
	rec := httptest.NewRecorder()

	s.root(rec, httptest.NewRequest(http.MethodGet, "/some/path", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if body["service"] != "echo" {
		t.Errorf("service = %v, want echo", body["service"])
	}
	// The pod name is how you tell which replica answered when debugging a
	// rollout; an empty one makes the endpoint useless for that.
	if pod, _ := body["pod"].(string); pod == "" {
		t.Error("pod is empty")
	}
	if body["path"] != "/some/path" {
		t.Errorf("path = %v, want /some/path", body["path"])
	}
}

func TestEnvOrFallsBack(t *testing.T) {
	t.Setenv("MLP_TEST_ENV", "")
	if got := envOr("MLP_TEST_ENV", "fallback"); got != "fallback" {
		t.Errorf("empty var gave %q, want fallback", got)
	}
	t.Setenv("MLP_TEST_ENV", "set")
	if got := envOr("MLP_TEST_ENV", "fallback"); got != "set" {
		t.Errorf("set var gave %q, want set", got)
	}
}

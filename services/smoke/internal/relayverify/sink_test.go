package relayverify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestSinkClientLifecycleAndDeliveryShapes(t *testing.T) {
	t.Parallel()

	var resetControl bool
	var cleared bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/healthz":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/control":
			var control struct {
				LatencyMS int     `json:"latency_ms"`
				FailRate  float64 `json:"fail_rate"`
			}
			if err := json.NewDecoder(r.Body).Decode(&control); err != nil {
				t.Errorf("decode control request: %v", err)
			}
			resetControl = control.LatencyMS == 0 && control.FailRate == 0
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodDelete && r.URL.Path == "/received":
			cleared = true
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/received":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"deliveries":[
				{"received_at":"2026-09-06T12:00:00Z","path":"/hooks/ok","data":{"seq":1,"marker":"run"},"status":200},
				{"received_at":"2026-09-06T12:00:01Z","path":"/hooks/ok","data":"{\"seq\":2,\"marker\":\"run\"}","status":202},
				{"received_at":"2026-09-06T12:00:02Z","path":"/hooks/ok","data":"not-json","status":200}
			]}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := SinkClient{BaseURL: server.URL + "/", HTTPClient: server.Client()}
	ctx := context.Background()
	if err := client.Health(ctx); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if err := client.ResetControl(ctx); err != nil {
		t.Fatalf("ResetControl() error = %v", err)
	}
	if err := client.ClearReceived(ctx); err != nil {
		t.Fatalf("ClearReceived() error = %v", err)
	}
	deliveries, err := client.Received(ctx)
	if err != nil {
		t.Fatalf("Received() error = %v", err)
	}

	if !resetControl {
		t.Error("ResetControl() did not send zero latency and failure rate")
	}
	if !cleared {
		t.Error("ClearReceived() did not delete delivery history")
	}
	if len(deliveries) != 3 {
		t.Fatalf("Received() returned %d deliveries, want 3", len(deliveries))
	}
	if got := extractSequences(deliveries, "run"); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Errorf("decoded sequences = %v, want [1 2]", got)
	}
	if deliveries[2].Data != nil {
		t.Errorf("invalid string data decoded as %v, want nil", deliveries[2].Data)
	}
}

func TestSinkClientReportsUnexpectedStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := (SinkClient{BaseURL: server.URL, HTTPClient: server.Client()}).Health(context.Background())
	if err == nil {
		t.Fatal("Health() error = nil, want unexpected status error")
	}
}

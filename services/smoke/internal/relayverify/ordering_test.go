package relayverify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

func TestOrderingPinsPreflightResetAndSequentialPosts(t *testing.T) {
	t.Parallel()

	type postedEvent struct {
		TenantID string `json:"tenant_id"`
		Type     string `json:"type"`
		Data     struct {
			Sequence int    `json:"seq"`
			Marker   string `json:"marker"`
		} `json:"data"`
	}

	var calls []string
	var events []postedEvent
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls = append(calls, request.Method+" "+request.URL.String())
		switch request.Method + " " + request.URL.String() {
		case "GET http://localhost:8082/readyz",
			"GET http://sink.test/healthz",
			"POST http://sink.test/control":
			return testHTTPResponse(request, http.StatusOK, `{}`), nil
		case "DELETE http://sink.test/received":
			return testHTTPResponse(request, http.StatusNoContent, ""), nil
		case "POST http://localhost:8082/v1/events":
			var event postedEvent
			if err := json.NewDecoder(request.Body).Decode(&event); err != nil {
				return nil, fmt.Errorf("decode posted event: %w", err)
			}
			events = append(events, event)
			return testHTTPResponse(request, http.StatusAccepted, ""), nil
		case "GET http://sink.test/received":
			deliveries := make([]map[string]any, 0, len(events))
			for _, event := range events {
				deliveries = append(deliveries, map[string]any{
					"path":   "/hooks/ok",
					"status": http.StatusOK,
					"data": map[string]any{
						"seq":    event.Data.Sequence,
						"marker": event.Data.Marker,
					},
				})
			}
			body, err := json.Marshal(map[string]any{"deliveries": deliveries})
			if err != nil {
				return nil, fmt.Errorf("encode sink response: %w", err)
			}
			return testHTTPResponse(request, http.StatusOK, string(body)), nil
		default:
			return testHTTPResponse(request, http.StatusNotFound, "unexpected request"), nil
		}
	})}

	err := Ordering(context.Background(), platform.Config{
		// Empty is deliberate: the dedicated verifier must match the shell's
		// default even though platform.Load preserves empty for make smoke.
		RelayIngestURL: "",
		SinkURL:        "http://sink.test",
	}, OrderingOptions{
		Events:       2,
		Tenant:       "globex",
		Timeout:      100 * time.Millisecond,
		PollInterval: time.Millisecond,
		HTTPClient:   client,
		Output:       io.Discard,
	})
	if err != nil {
		t.Fatalf("Ordering() error = %v", err)
	}

	wantCalls := []string{
		"GET http://localhost:8082/readyz",
		"GET http://sink.test/healthz",
		"POST http://sink.test/control",
		"DELETE http://sink.test/received",
		"POST http://localhost:8082/v1/events",
		"POST http://localhost:8082/v1/events",
		"GET http://sink.test/received",
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Errorf("request order = %v, want %v", calls, wantCalls)
	}
	if len(events) != 2 {
		t.Fatalf("posted %d events, want 2", len(events))
	}
	for index, event := range events {
		if event.TenantID != "globex" || event.Type != "ordering.check" {
			t.Errorf("event %d metadata = tenant %q, type %q", index+1, event.TenantID, event.Type)
		}
		if event.Data.Sequence != index+1 {
			t.Errorf("event %d sequence = %d", index+1, event.Data.Sequence)
		}
		if event.Data.Marker == "" || event.Data.Marker != events[0].Data.Marker {
			t.Errorf("event %d marker = %q, first marker %q", index+1, event.Data.Marker, events[0].Data.Marker)
		}
	}
}

func TestExtractSequencesFiltersByPathStatusAndMarker(t *testing.T) {
	t.Parallel()

	deliveries := []Delivery{
		delivery("/hooks/ok", 200, "run", 1),
		delivery("/hooks/flaky", 200, "run", 2),
		delivery("/hooks/ok", 500, "run", 3),
		delivery("/hooks/ok", 204, "another-run", 4),
		delivery("/hooks/ok", 299, "run", 5),
		{Path: "/hooks/ok", Status: 200, Data: map[string]json.RawMessage{"marker": json.RawMessage(`"run"`)}},
	}

	if got := extractSequences(deliveries, "run"); !reflect.DeepEqual(got, []int{1, 5}) {
		t.Errorf("extractSequences() = %v, want [1 5]", got)
	}
}

func TestCompareOrderReportsFirstDivergence(t *testing.T) {
	t.Parallel()

	err := compareOrder([]int{1, 2, 4, 3, 5}, 5)
	if err == nil || err.Error() != "position 2: delivered seq 4, expected 3" {
		t.Fatalf("compareOrder() error = %v", err)
	}
}

func TestCompareOrderReportsDifferentLengths(t *testing.T) {
	t.Parallel()

	err := compareOrder([]int{1, 2}, 3)
	if err == nil || err.Error() != "lengths differ: 2 delivered, 3 expected" {
		t.Fatalf("compareOrder() error = %v", err)
	}
}

func TestAwaitSequencesPollsUntilAllEventsArrive(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count := 1
		if requests.Add(1) >= 2 {
			count = 2
		}
		fmt.Fprintf(w, `{"deliveries":[{"path":"/hooks/ok","data":{"seq":1,"marker":"run"},"status":200}`)
		if count == 2 {
			fmt.Fprint(w, `,{"path":"/hooks/ok","data":{"seq":2,"marker":"run"},"status":200}`)
		}
		fmt.Fprint(w, `]}`)
	}))
	defer server.Close()

	got, err := awaitSequences(
		context.Background(),
		SinkClient{BaseURL: server.URL, HTTPClient: server.Client()},
		"run",
		2,
		200*time.Millisecond,
		time.Millisecond,
	)
	if err != nil {
		t.Fatalf("awaitSequences() error = %v", err)
	}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Errorf("awaitSequences() = %v, want [1 2]", got)
	}
	if requests.Load() < 2 {
		t.Errorf("received %d poll requests, want at least 2", requests.Load())
	}
}

func TestAwaitSequencesTimesOutWithDeliveredCount(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"deliveries":[{"path":"/hooks/ok","data":{"seq":1,"marker":"run"},"status":200}]}`)
	}))
	defer server.Close()

	got, err := awaitSequences(
		context.Background(),
		SinkClient{BaseURL: server.URL, HTTPClient: server.Client()},
		"run",
		2,
		25*time.Millisecond,
		2*time.Millisecond,
	)
	if !reflect.DeepEqual(got, []int{1}) {
		t.Errorf("awaitSequences() = %v, want [1]", got)
	}
	if err == nil || !strings.Contains(err.Error(), "only 1 of 2 were delivered within 25ms") {
		t.Fatalf("awaitSequences() error = %v", err)
	}
}

func TestParseOrderingOptionsUsesEnvironment(t *testing.T) {
	t.Setenv("EVENTS", "12")
	t.Setenv("TENANT", "environment-tenant")

	opts, err := ParseOrderingOptions(nil)
	if err != nil {
		t.Fatalf("ParseOrderingOptions() error = %v", err)
	}
	if opts.Events != 12 || opts.Tenant != "environment-tenant" {
		t.Errorf("ParseOrderingOptions() = events %d, tenant %q", opts.Events, opts.Tenant)
	}
}

func TestParseOrderingOptionsFlagsOverrideEnvironment(t *testing.T) {
	t.Setenv("EVENTS", "12")
	t.Setenv("TENANT", "environment-tenant")

	opts, err := ParseOrderingOptions([]string{"--events", "7", "--tenant", "flag-tenant"})
	if err != nil {
		t.Fatalf("ParseOrderingOptions() error = %v", err)
	}
	if opts.Events != 7 || opts.Tenant != "flag-tenant" {
		t.Errorf("ParseOrderingOptions() = events %d, tenant %q", opts.Events, opts.Tenant)
	}
}

func TestParseOrderingOptionsDefaults(t *testing.T) {
	t.Setenv("EVENTS", "")
	t.Setenv("TENANT", "")

	opts, err := ParseOrderingOptions(nil)
	if err != nil {
		t.Fatalf("ParseOrderingOptions() error = %v", err)
	}
	if opts.Events != 40 || opts.Tenant != "globex" {
		t.Errorf("ParseOrderingOptions() = events %d, tenant %q", opts.Events, opts.Tenant)
	}
}

func delivery(path string, status int, marker string, sequence int) Delivery {
	markerJSON, _ := json.Marshal(marker)
	sequenceJSON, _ := json.Marshal(sequence)
	return Delivery{
		Path:   path,
		Status: status,
		Data: map[string]json.RawMessage{
			"marker": markerJSON,
			"seq":    sequenceJSON,
		},
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func testHTTPResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

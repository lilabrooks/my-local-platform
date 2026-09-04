package checks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

func TestRelayEmptyURLIsAnExplicitSkip(t *testing.T) {
	t.Parallel()

	detail, err := Relay(platform.Config{RelayIngestURL: ""}).Run(context.Background())
	if err != nil {
		t.Fatalf("Relay returned an error for a disabled check: %v", err)
	}
	if detail != "skipped (apps profile disabled)" {
		t.Fatalf("detail = %q, want an explicit skip", detail)
	}
}

// completeTraceJSON is one relay path: ingest, produce, consume, and two
// attempt spans for subscription 7. It carries decoys for every way this parse
// could match the wrong thing -- another event's span, a non-relay span name,
// and an attempt span belonging to a different event.
const completeTraceJSON = `{"batches":[{"scopeSpans":[{"spans":[
	{"name":"relay.ingest","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]},
	{"name":"kafka.produce","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]},
	{"name":"relay.consume","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]},
	{"name":"relay.webhook.attempt","attributes":[
		{"key":"relay.event.id","value":{"stringValue":"evt_test"}},
		{"key":"relay.subscription.id","value":{"intValue":"7"}},
		{"key":"relay.delivery.attempt","value":{"intValue":"1"}}]},
	{"name":"relay.webhook.attempt","attributes":[
		{"key":"relay.event.id","value":{"stringValue":"evt_test"}},
		{"key":"relay.subscription.id","value":{"intValue":"7"}},
		{"key":"relay.delivery.attempt","value":{"intValue":"2"}}]},
	{"name":"relay.consume","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_other"}}]},
	{"name":"relay.webhook.attempt","attributes":[
		{"key":"relay.event.id","value":{"stringValue":"evt_other"}},
		{"key":"relay.subscription.id","value":{"intValue":"7"}},
		{"key":"relay.delivery.attempt","value":{"intValue":"1"}}]},
	{"name":"unrelated","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]}
]}]}]}`

func tempoServer(t *testing.T, traceJSON string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/traces/0102" {
			t.Errorf("path = %q, want /api/traces/0102", r.URL.Path)
		}
		_, _ = w.Write([]byte(traceJSON))
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func TestFetchRelayTraceSelectsNamedSpansForTheEvent(t *testing.T) {
	t.Parallel()

	got, err := fetchRelayTrace(context.Background(), tempoServer(t, completeTraceJSON), "0102", "evt_test")
	if err != nil {
		t.Fatalf("fetchRelayTrace: %v", err)
	}
	for name, want := range map[string]int{
		"relay.ingest":          1,
		"kafka.produce":         1,
		"relay.consume":         1,
		"relay.webhook.attempt": 2,
		"unrelated":             0,
	} {
		if got.counts[name] != want {
			t.Errorf("%s spans = %d, want %d", name, got.counts[name], want)
		}
	}
	// The evt_other attempt span shares subscription 7 and attempt 1 with a
	// span this event does not have. Keying on the pair alone, without first
	// filtering by event id, would report it as ours.
	want := map[attemptKey]int{{7, 1}: 1, {7, 2}: 1}
	if len(got.attempts) != len(want) {
		t.Fatalf("attempt spans = %v, want %v", got.attempts, want)
	}
	for key, n := range want {
		if got.attempts[key] != n {
			t.Errorf("subscription %d attempt %d = %d spans, want %d",
				key.SubscriptionID, key.AttemptNumber, got.attempts[key], n)
		}
	}
}

// One span per persisted attempt is the assertion; the previous version of this
// check passed on any single relay.webhook.attempt span, which is exactly the
// collapse it was meant to catch.
func TestAwaitRelayTraceRequiresOneSpanPerPersistedAttempt(t *testing.T) {
	t.Parallel()

	twoAttempts := []deliveryAttempt{
		{SubscriptionID: 7, AttemptNumber: 1},
		{SubscriptionID: 7, AttemptNumber: 2},
	}
	threeAttempts := append(append([]deliveryAttempt{}, twoAttempts...),
		deliveryAttempt{SubscriptionID: 9, AttemptNumber: 1})

	oneAttemptSpan := `{"batches":[{"scopeSpans":[{"spans":[
		{"name":"relay.ingest","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]},
		{"name":"kafka.produce","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]},
		{"name":"relay.consume","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]},
		{"name":"relay.webhook.attempt","attributes":[
			{"key":"relay.event.id","value":{"stringValue":"evt_test"}},
			{"key":"relay.subscription.id","value":{"intValue":"7"}},
			{"key":"relay.delivery.attempt","value":{"intValue":"1"}}]}
	]}]}]}`
	noConsume := `{"batches":[{"scopeSpans":[{"spans":[
		{"name":"relay.ingest","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]},
		{"name":"kafka.produce","attributes":[{"key":"relay.event.id","value":{"stringValue":"evt_test"}}]}
	]}]}]}`

	for _, tc := range []struct {
		name      string
		traceJSON string
		attempts  []deliveryAttempt
		wantErr   string
	}{
		{"complete", completeTraceJSON, twoAttempts, ""},
		{"one span for two attempts", oneAttemptSpan, twoAttempts, "subscription 7 attempt 2 has 0 spans"},
		{"missing subscription", completeTraceJSON, threeAttempts, "subscription 9 attempt 1 has 0 spans"},
		{"no consume span", noConsume, twoAttempts, "no relay.consume span"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer cancel()
			err := awaitRelayTrace(ctx, tempoServer(t, tc.traceJSON), "0102", "evt_test", tc.attempts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("awaitRelayTrace = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("awaitRelayTrace = nil, want an error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("awaitRelayTrace = %v, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// An unreachable Tempo must say so rather than reporting the trace as
// incomplete: the two failures have different fixes.
func TestAwaitRelayTraceReportsAnUnreadableTempo(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	err := awaitRelayTrace(ctx, "http://127.0.0.1:1", "0102", "evt_test",
		[]deliveryAttempt{{SubscriptionID: 7, AttemptNumber: 1}})
	if err == nil || !strings.Contains(err.Error(), "never readable") {
		t.Fatalf("awaitRelayTrace = %v, want a 'never readable' error", err)
	}
}

func TestCheckAttemptHistory(t *testing.T) {
	t.Parallel()

	valid := []deliveryAttempt{
		{SubscriptionID: 1, SubscriptionURL: "http://sink:8081/hooks/ok", AttemptNumber: 1, Outcome: "delivered"},
		{SubscriptionID: 2, SubscriptionURL: "http://sink:8081/hooks/flaky", AttemptNumber: 1, Outcome: "retrying"},
		{SubscriptionID: 2, SubscriptionURL: "http://sink:8081/hooks/flaky", AttemptNumber: 2, Outcome: "retrying"},
		{SubscriptionID: 2, SubscriptionURL: "http://sink:8081/hooks/flaky", AttemptNumber: 3, Outcome: "exhausted"},
	}
	if err := checkAttemptHistory("evt_test", valid); err != nil {
		t.Fatalf("valid history: %v", err)
	}

	badOrder := append([]deliveryAttempt(nil), valid...)
	badOrder[3].AttemptNumber = 2
	if err := checkAttemptHistory("evt_test", badOrder); err == nil || !strings.Contains(err.Error(), "follows") {
		t.Errorf("duplicate attempt number error = %v, want ordering failure", err)
	}

	if err := checkAttemptHistory("evt_test", valid[:1]); err == nil {
		t.Error("history without the exhausted subscriber was accepted")
	}
	if err := checkAttemptHistory("evt_test", valid[:3]); err == nil || !strings.Contains(err.Error(), "exactly 4") {
		t.Errorf("short history error = %v, want exact-count failure", err)
	}
}

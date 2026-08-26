package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatusClass(t *testing.T) {
	tests := map[int]string{
		200: "2xx",
		204: "2xx",
		301: "3xx",
		404: "4xx",
		500: "5xx",
		599: "5xx",
		// No response at all: a refused connection, a DNS failure, or an
		// attempt that hit RELAY_DELIVERY_TIMEOUT. Folding these into 5xx
		// would hide the difference between "the subscriber is broken" and
		// "the subscriber is not answering", which need different fixes.
		0:  "error",
		-1: "error",
		// Nothing should produce these, but a label of "6xx" would be worse
		// than one that says the value was not a status.
		42:  "unknown",
		700: "unknown",
	}

	for status, want := range tests {
		if got := StatusClass(status); got != want {
			t.Errorf("StatusClass(%d) = %q, want %q", status, got, want)
		}
	}
}

func TestHandlerServesTheRegisteredFamilies(t *testing.T) {
	// Touch one series in each family so it is present in the output. A
	// counter vec with no observed label set exports nothing, which is correct
	// but makes for a thin assertion.
	IngestEvents.WithLabelValues("accepted").Add(0)
	DeliveryAttempts.WithLabelValues("2xx").Add(0)
	// A histogram vec with no observation exports no buckets at all, so this
	// one has to be observed rather than merely touched.
	AttemptDuration.WithLabelValues("2xx").Observe(0)
	Deliveries.WithLabelValues("delivered").Add(0)
	DeadLetters.WithLabelValues("exhausted").Add(0)
	RecordsConsumed.WithLabelValues("0").Add(0)
	BuildInfo.WithLabelValues("test", "ingest").Set(1)
	ConsumerLag.WithLabelValues(testGroup, testTopic, "0").Set(0)

	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	// These are the names Prometheus scrapes and the dashboard queries. A
	// rename that slips through breaks the panel silently, which is the whole
	// failure mode issue #22 exists to close, so the names are asserted here
	// rather than left to be discovered on a demo run.
	for _, name := range []string{
		"relay_build_info",
		"relay_ingest_events_total",
		"relay_delivery_attempts_total",
		"relay_delivery_attempt_duration_seconds_bucket",
		"relay_deliveries_total",
		"relay_dead_letters_total",
		"relay_records_consumed_total",
		"relay_record_duration_seconds_bucket",
		"relay_consumer_group_lag",
		"relay_consumer_group_lag_total",
		"relay_lag_refreshed_timestamp_seconds",
		// Registered explicitly rather than inherited from the default
		// registry, so worth confirming they actually arrived.
		"go_goroutines",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("scrape output is missing %s", name)
		}
	}
}

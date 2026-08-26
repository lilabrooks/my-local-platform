// Package metrics is relay's Prometheus instrumentation.
//
// Unlike services/echo, which hand-writes two counters rather than take on a
// client library, relay needs labelled counters, a latency histogram and a set
// of per-partition gauges. Bucket arithmetic and label cardinality are exactly
// what the library exists to get right, and the numbers here are the evidence
// the M2 demo rests on -- a subtly wrong histogram would be worse than none.
// services/sink stays standard-library-only; it needs counters and nothing more.
//
// Everything registers into a package registry rather than the default one, so
// a test can gather a deterministic set without whatever else a process has
// registered globally.
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds every metric this service exports.
var Registry = prometheus.NewRegistry()

func init() {
	// Go runtime and process metrics are worth the bytes here: both roles run
	// under a 128m limit in compose and a memory limit in Kubernetes, so
	// "is it about to be OOM-killed" is a question the demo can be asked.
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

// Handler serves the registry in Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{
		// A collector that errors should say so in the response rather than
		// serve a partial scrape that reads as "everything is zero".
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

// factory registers into Registry rather than the default one.
var factory = promauto.With(Registry)

var (
	// BuildInfo is always 1. Its value is not the point: the series exists once
	// per running process, so `count(relay_build_info{role="deliver"})` is the
	// consumer count, which is the second line on the demo's lag panel.
	BuildInfo = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_build_info",
		Help: "Always 1. Labelled with the version and role of a running relay process.",
	}, []string{"version", "role"})

	// IngestEvents counts what happened to each POST /v1/events.
	//
	// Outcome rather than HTTP status because the distinction that matters is
	// "durably on the log" against everything else, and two different statuses
	// (400 and 413) mean the same thing to an operator.
	IngestEvents = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_ingest_events_total",
		Help: "Events submitted to POST /v1/events, by outcome.",
	}, []string{"outcome"})

	// DeliveryAttempts counts individual HTTP requests to subscribers, which
	// is what climbs when a subscriber starts failing and the retries begin.
	DeliveryAttempts = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_delivery_attempts_total",
		Help: "HTTP requests made to subscribers, by response status class.",
	}, []string{"status_class"})

	// AttemptDuration is one HTTP request to one subscriber.
	//
	// This is the histogram the demo reads: slowing the sink to 2s moves this
	// distribution, and that movement is the cause of the lag on the same
	// panel. Whole-record duration would mix it with retry sleeps.
	AttemptDuration = factory.NewHistogramVec(prometheus.HistogramOpts{
		Name: "relay_delivery_attempt_duration_seconds",
		Help: "Duration of one delivery attempt, from request to response.",
		// Reaching past RELAY_DELIVERY_TIMEOUT (2s by default) on purpose: a
		// bucket boundary at the timeout is what makes "these attempts are
		// timing out" distinguishable from "these attempts are merely slow",
		// which is the confusion that cost a demo run in docs/runbook-k8s.md.
		Buckets: []float64{0.005, 0.025, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	}, []string{"status_class"})

	// Deliveries counts events reaching a terminal state per subscriber.
	Deliveries = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_deliveries_total",
		Help: "Per-subscriber deliveries that reached a terminal state, by outcome.",
	}, []string{"outcome"})

	// DeadLetters is separated from Deliveries by reason, because "the
	// subscriber refused it five times" and "this record will never parse" are
	// different problems that a single dead-letter count would merge.
	DeadLetters = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_dead_letters_total",
		Help: "Records written to the dead-letter topic, by reason.",
	}, []string{"reason"})

	// RecordsConsumed is labelled by partition, so the demo can show work
	// spreading across partitions as KEDA adds consumers.
	//
	// This is NOT the same as "partitions this pod is assigned": a pod holding
	// an idle partition never increments it. Readiness reflecting real
	// assignment is issue #21.
	RecordsConsumed = factory.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_records_consumed_total",
		Help: "Records fetched from the delivery topic and processed to completion, by partition.",
	}, []string{"partition"})

	// RecordDuration covers the whole fan-out for one record, retry sleeps
	// included. It is what actually bounds throughput per consumer, and so the
	// number to reach for when lag is draining more slowly than expected.
	RecordDuration = factory.NewHistogram(prometheus.HistogramOpts{
		Name:    "relay_record_duration_seconds",
		Help:    "Time to take one record from fetch to commit, across every subscriber.",
		Buckets: []float64{0.01, 0.05, 0.25, 1, 2.5, 5, 10, 30, 60},
	})
)

// StatusClass buckets an HTTP status into the label used above.
//
// A status of 0 means no response was received at all -- a connection refused,
// a DNS failure, or an attempt that hit its timeout. That is a different
// condition from any server reply and gets its own class rather than being
// folded into 5xx.
func StatusClass(status int) string {
	if status <= 0 {
		return "error"
	}
	if status < 100 || status > 599 {
		return "unknown"
	}
	return strconv.Itoa(status/100) + "xx"
}

// Command relay is the webhook delivery service. One binary, two roles,
// selected by RELAY_MODE.
//
//	ingest   accept events over HTTP and produce them to Kafka
//	deliver  consume them and POST to subscribers, retrying and dead-lettering
//
// One image with a mode switch rather than two binaries, because the two roles
// share the record format and the configuration surface, and a Deployment per
// role is what actually separates them at runtime.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/delivery"
	"github.com/lilabrooks/my-local-platform/relay/internal/ingest"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
)

// Set at build time: -ldflags "-X main.version=..."
var version = "dev"

// These two were one constant until 2026-08-31, and collapsing them is what
// produced a wrong rationale that survived two corrections. They are 30s each
// by coincidence, not derivation, and they bound unrelated things.

// stallBudget is the longest one record may occupy this consumer. Service
// policy: head-of-line delay across every partition the member owns, and the
// pod's 45s termination grace period. Defined in config so k8s/validate checks
// manifests against the same value.
const stallBudget = config.DefaultStallBudget

// rebalanceTimeout is the Kafka protocol field of that name -- how long the
// coordinator waits for members to send JoinGroup during a rebalance. It has
// nothing to do with how long a handler runs: kafka-go's group management runs
// on its own goroutines, so a busy consumer still rejoins. Set explicitly to
// kafka-go's own default rather than left implicit.
// See docs/adr/0006-kafka-over-sqs-for-delivery.md.
const rebalanceTimeout = 30 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "relay", "version", version)
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	mode := envOr("RELAY_MODE", "ingest")

	// Validated in every mode, not just deliver. Both Deployments read the
	// same ConfigMap, so a typo should stop the rollout rather than pass in
	// one role and fail in the other.
	schedule, err := config.ParseRetrySchedule(
		envOr("RELAY_RETRY_DELAYS", "demo"),
		envOr("RELAY_RETRY_JITTER", "true") == "true",
	)
	if err != nil {
		return fmt.Errorf("RELAY_RETRY_DELAYS: %w", err)
	}
	attemptTimeout, err := time.ParseDuration(envOr("RELAY_DELIVERY_TIMEOUT", "2s"))
	if err != nil {
		return fmt.Errorf("RELAY_DELIVERY_TIMEOUT: %w", err)
	}
	if err := schedule.ValidateStallBudget(stallBudget, attemptTimeout); err != nil {
		return err
	}
	log.Info("retry schedule",
		"schedule", schedule.String(),
		"attempt_timeout", attemptTimeout,
		"worst_case_per_record", schedule.WorstCase(attemptTimeout),
		"stall_budget", stallBudget)

	switch mode {
	case "ingest":
		return runIngest(log)
	case "deliver":
		return runDeliver(log, schedule, attemptTimeout)
	default:
		return fmt.Errorf("RELAY_MODE %q is not one of: ingest, deliver", mode)
	}
}

func brokers() []string { return strings.Split(envOr("KAFKA_BOOTSTRAP", "localhost:9092"), ",") }

func runIngest(log *slog.Logger) error {
	topic := envOr("RELAY_TOPIC", "mlp.relay.deliveries")
	addr := ":" + envOr("PORT", "8080")
	metrics.BuildInfo.WithLabelValues(version, "ingest").Set(1)

	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers()...),
		Topic: topic,
		// Hash on the record key so a tenant always lands on one partition.
		// LeastBytes would spread a tenant across partitions and lose ordering.
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false, // topics come from bootstrap, not by accident
		// The default is 1s waiting for a batch of 100 that a low-rate ingest
		// never fills -- the same default that cost the smoke check a flat
		// second per run. 10ms still batches under load.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer func() { _ = writer.Close() }()

	server := ingest.New(writer, topic, log)
	// Safe to be ready immediately: kafka.Writer connects lazily, and an
	// unreachable broker surfaces as a 503 per request rather than at startup.
	server.MarkReady(true)

	srv := &http.Server{Addr: addr, Handler: server.Routes(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Consumer group lag is published here rather than by the consumers, even
	// though it is entirely about them.
	//
	// A deliver pod knows only its own partitions, and KEDA moves that group
	// between one and twelve members during the demo -- so per-pod lag series
	// come and go, and their sum is least trustworthy exactly when someone is
	// watching it. ingest already holds a broker connection, so it can read the
	// group's committed offsets straight from the broker and publish a series
	// per partition that does not move when the group rebalances. That is also
	// where KEDA reads lag, which is why the panel and the scaler agree by
	// construction rather than by coincidence.
	//
	// EVERY INGEST REPLICA RUNS THIS, and the Deployment runs two. They poll
	// the same group and publish the same numbers, which is harmless -- the
	// value is a property of the group, not of the pod -- but it means anything
	// consuming these series must aggregate with `max`, never `sum`. Summing
	// multiplies lag by the replica count. The dashboard got this wrong until a
	// cluster run showed a peak of 1103 for 600 events produced; see
	// docs/adr/0008-in-cluster-observability-for-the-demo.md.
	go metrics.NewLagPoller(
		brokers(),
		envOr("RELAY_CONSUMER_GROUP", "relay-deliver"),
		topic,
		lagInterval(log),
		log,
	).Run(ctx)

	go func() {
		<-ctx.Done()
		log.Info("shutdown signal received, failing readiness")
		server.MarkReady(false)
		time.Sleep(3 * time.Second)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("graceful shutdown failed", "error", err)
		}
	}()

	log.Info("listening", "addr", addr, "topic", topic, "brokers", brokers(), "mode", "ingest")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}
	log.Info("stopped")
	return nil
}

func runDeliver(log *slog.Logger, schedule config.RetrySchedule, attemptTimeout time.Duration) error {
	metrics.BuildInfo.WithLabelValues(version, "deliver").Set(1)
	topic := envOr("RELAY_TOPIC", "mlp.relay.deliveries")
	dlqTopic := envOr("RELAY_DLQ_TOPIC", "mlp.relay.deliveries.dlq")
	group := envOr("RELAY_CONSUMER_GROUP", "relay-deliver")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, envOr("DATABASE_URL",
		"postgres://platform:platform@localhost:5432/platform?sslmode=disable"))
	if err != nil {
		return fmt.Errorf("database pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		// Unlike the writer, the subscription store is needed for every
		// record, so a database that is not there at startup is fatal rather
		// than a per-record failure.
		return fmt.Errorf("database unreachable: %w", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers(),
		Topic:   topic,
		GroupID: group,
		// CommitInterval zero means CommitMessages commits synchronously,
		// which is what makes "commit only after every subscriber is done"
		// mean anything.
		CommitInterval:   0,
		RebalanceTimeout: rebalanceTimeout,
		// Without this, a consumer that joins before its topic exists is
		// assigned zero partitions and never notices when they appear -- it
		// sits in the group consuming nothing, looking healthy. That is
		// exactly what happened in CI, where kafka-topics.sh runs after
		// `docker compose up`: the group stabilised with one member and no
		// assignment, and every event produced afterwards went undelivered.
		//
		// Topics are created by local/bootstrap/kafka-topics.sh and auto
		// creation is off, so a consumer outliving a topic change is a normal
		// startup ordering, not an exotic case.
		WatchPartitionChanges:  true,
		PartitionWatchInterval: 2 * time.Second,
		MinBytes:               1,
		MaxBytes:               10e6,
		MaxWait:                250 * time.Millisecond,
		// A new group starts at the beginning so nothing already produced is
		// skipped on first deploy.
		StartOffset: kafka.FirstOffset,
	})
	defer func() { _ = reader.Close() }()

	dlq := &kafka.Writer{
		Addr:                   kafka.TCP(brokers()...),
		Topic:                  dlqTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer func() { _ = dlq.Close() }()

	consumer := delivery.NewConsumer(
		reader, dlq, subscriptions.New(pool),
		delivery.NewDeliverer(schedule, attemptTimeout), log,
	)

	// Health endpoints run alongside the consume loop: a consumer has no
	// inbound traffic, but Kubernetes still needs to know it is alive.
	srv := healthServer(":"+envOr("PORT", "8080"), consumer)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server", "error", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("consuming", "topic", topic, "dlq", dlqTopic, "group", group,
		"brokers", brokers(), "mode", "deliver")
	return consumer.Run(ctx)
}

func healthServer(addr string, c *delivery.Consumer) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !c.Ready() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "stats": c.Stats()})
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// lagInterval bounds itself rather than trusting the environment: a poll every
// few milliseconds would hammer the broker with three requests a round, and one
// every ten minutes would make the demo's lag panel a flat line that moves
// after the interesting part is over.
func lagInterval(log *slog.Logger) time.Duration {
	const def = 5 * time.Second
	raw := envOr("RELAY_LAG_INTERVAL", def.String())
	d, err := time.ParseDuration(raw)
	switch {
	case err != nil:
		log.Warn("RELAY_LAG_INTERVAL is not a duration, using the default", "value", raw, "default", def)
		return def
	case d < time.Second, d > time.Minute:
		log.Warn("RELAY_LAG_INTERVAL is outside 1s..1m, using the default", "value", d, "default", def)
		return def
	}
	return d
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

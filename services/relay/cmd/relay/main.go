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
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
)

// Set at build time: -ldflags "-X main.version=..."
var version = "dev"

// rebalanceTimeout is given to the consumer group and is the bound the retry
// schedule is validated against: a consumer busy with one record for longer
// than this cannot rejoin its group after a rebalance, so the delivery is
// reassigned. See docs/adr/0006-kafka-over-sqs-for-delivery.md.
//
// Defined in config so k8s/validate checks manifests against the same value.
const rebalanceTimeout = config.DefaultRebalanceTimeout

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
	if err := schedule.ValidateLiveness(rebalanceTimeout, attemptTimeout); err != nil {
		return err
	}
	log.Info("retry schedule",
		"schedule", schedule.String(),
		"attempt_timeout", attemptTimeout,
		"worst_case_per_record", schedule.WorstCase(attemptTimeout),
		"rebalance_timeout", rebalanceTimeout)

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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

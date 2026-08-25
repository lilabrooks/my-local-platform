// Command relay is the webhook delivery service. One binary, two roles,
// selected by RELAY_MODE.
//
//	ingest   accept events over HTTP and produce them to Kafka
//	deliver  consume them and POST to subscribers (issue #6, not yet built)
//
// One image with a mode switch rather than two binaries, because the two roles
// share the record format and the configuration surface, and a Deployment per
// role is what actually separates them at runtime.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/ingest"
)

// Set at build time: -ldflags "-X main.version=..."
var version = "dev"

// rebalanceTimeout must match the delivery consumer's kafka-go ReaderConfig.
// It is the bound the retry schedule is validated against: a consumer asleep
// in a retry for longer than this cannot rejoin its group after a rebalance.
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

	// The retry schedule is validated in every mode, not just deliver. Both
	// Deployments read the same ConfigMap, so a typo should stop the rollout
	// rather than pass in one role and fail in the other.
	schedule, err := config.ParseRetrySchedule(envOr("RELAY_RETRY_DELAYS", "demo"), envOr("RELAY_RETRY_JITTER", "true") == "true")
	if err != nil {
		return fmt.Errorf("RELAY_RETRY_DELAYS: %w", err)
	}
	if err := schedule.ValidateLiveness(rebalanceTimeout); err != nil {
		return err
	}
	// Logged so the longest an event can sit before dead-lettering is a fact an
	// operator reads rather than computes.
	log.Info("retry schedule", "schedule", schedule.String())

	switch mode {
	case "ingest":
		return runIngest(log, schedule)
	case "deliver":
		return errors.New("RELAY_MODE=deliver is not implemented yet (issue #6)")
	default:
		return fmt.Errorf("RELAY_MODE %q is not one of: ingest, deliver", mode)
	}
}

func runIngest(log *slog.Logger, _ config.RetrySchedule) error {
	brokers := strings.Split(envOr("KAFKA_BOOTSTRAP", "localhost:9092"), ",")
	topic := envOr("RELAY_TOPIC", "mlp.relay.deliveries")
	addr := ":" + envOr("PORT", "8080")

	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
		// Hash on the record key so a tenant always lands on one partition.
		// LeastBytes would spread a tenant across partitions and lose ordering.
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false, // topics come from bootstrap, not by accident
		// The default is 1s, and a batch of 100 that never fills makes every
		// request wait it out -- the same default that cost the smoke check a
		// flat second per run. 10ms still batches under load.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer func() { _ = writer.Close() }()

	server := newServer(writer, topic, log)
	srv := &http.Server{
		Addr:              addr,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return serve(srv, server, log, addr, topic, brokers)
}

// newServer exists so runIngest reads in one direction; it also marks the
// server ready, which is safe because kafka.Writer connects lazily and an
// unreachable broker surfaces as a 503 per request rather than at startup.
func newServer(w ingest.Producer, topic string, log *slog.Logger) *ingest.Server {
	s := ingest.New(w, topic, log)
	s.MarkReady(true)
	return s
}

func serve(srv *http.Server, server *ingest.Server, log *slog.Logger, addr, topic string, brokers []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		// Fail readiness first so the endpoint leaves its Service before
		// connections stop being accepted, then drain.
		log.Info("shutdown signal received, failing readiness")
		server.MarkReady(false)
		time.Sleep(3 * time.Second)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Error("graceful shutdown failed", "error", err)
		}
	}()

	log.Info("listening", "addr", addr, "topic", topic, "brokers", brokers, "mode", "ingest")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}
	log.Info("stopped")
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

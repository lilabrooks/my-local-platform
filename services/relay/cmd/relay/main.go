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
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/delivery"
	"github.com/lilabrooks/my-local-platform/relay/internal/history"
	"github.com/lilabrooks/my-local-platform/relay/internal/ingest"
	"github.com/lilabrooks/my-local-platform/relay/internal/kafkatransport"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
	"github.com/lilabrooks/my-local-platform/relay/internal/telemetry"
)

// Set at build time: -ldflags "-X main.version=..."
var version = "dev"

// These two were one constant until 2026-08-31, and collapsing them is what
// produced a wrong rationale that survived two corrections. They are 30s each
// by coincidence, not derivation, and they bound unrelated things.

// stallBudget is the longest one record may occupy this consumer. Service
// policy: head-of-line delay across every partition the member owns, and the
// pod's termination grace period, which must also contain every shutdown step
// that follows the drain -- see config.DeliverDrainBudget. Defined in config so
// k8s/validate checks manifests against the same values.
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
	if mode != "ingest" && mode != "deliver" {
		return fmt.Errorf("RELAY_MODE %q is not one of: ingest, deliver", mode)
	}

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

	kafkaConnection, err := kafkatransport.New(
		context.Background(),
		envOr("KAFKA_BOOTSTRAP", "localhost:9092"),
		envOr("KAFKA_AUTH_MODE", kafkatransport.AuthNone),
		os.Getenv("AWS_REGION"),
	)
	if err != nil {
		return fmt.Errorf("kafka transport: %w", err)
	}

	shutdownTracing, err := telemetry.Init(
		context.Background(),
		envOr("OTEL_SERVICE_NAME", "relay-"+mode),
		envOr("DEPLOYMENT_ENVIRONMENT", "local"),
		envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
	)
	if err != nil {
		return fmt.Errorf("initialize tracing: %w", err)
	}
	// Runs last of every shutdown step, so its bound is the final term in the
	// drain budget k8s/validate checks against terminationGracePeriodSeconds.
	// An unreachable collector spends all of it; see config.TraceFlushTimeout.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), config.TraceFlushTimeout)
		defer cancel()
		if err := shutdownTracing(ctx); err != nil {
			log.Error("flush traces", "error", err)
		}
	}()

	switch mode {
	case "ingest":
		return runIngest(log, kafkaConnection)
	case "deliver":
		return runDeliver(log, kafkaConnection, schedule, attemptTimeout)
	}
	return nil
}

// closeBounded runs closers in reverse registration order -- the order defer
// would have used -- and gives up after config.ResourceCloseTimeout.
//
// None of what it closes accepts a context. kafka-go's Reader.Close leaves the
// consumer group, its Writer.Close flushes buffered messages, and
// pgxpool.Close waits for every connection to be returned; each can block
// indefinitely on a broker or database that has stopped answering. They run
// between the health server's shutdown and the trace flush, so an unbounded
// wait here is an unbounded wait inside the pod's termination grace period --
// which is the thing the drain budget exists to rule out, and which a plain
// `defer x.Close()` per resource quietly reintroduced.
//
// On timeout the goroutine is abandoned rather than waited for. The process is
// exiting; a goroutine parked on a dead socket outlives it by microseconds, and
// the alternative is the SIGKILL this is avoiding.
func closeBounded(log *slog.Logger, closers []func() error) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := len(closers) - 1; i >= 0; i-- {
			if err := closers[i](); err != nil {
				log.Warn("shutdown close failed", "error", err)
			}
		}
	}()
	timer := time.NewTimer(config.ResourceCloseTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		log.Warn("shutdown closes did not finish inside their budget",
			"budget", config.ResourceCloseTimeout)
	}
}

func runIngest(log *slog.Logger, kafkaConnection *kafkatransport.Connection) error {
	topic := envOr("RELAY_TOPIC", "mlp.relay.deliveries")
	addr := ":" + envOr("PORT", "8080")
	metrics.BuildInfo.WithLabelValues(version, "ingest").Set(1)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, envOr("DATABASE_URL",
		"postgres://platform:platform@localhost:5432/platform?sslmode=disable"))
	if err != nil {
		return fmt.Errorf("database pool: %w", err)
	}
	// One bounded close for every resource, registered before the HTTP
	// server's own shutdown so defer's LIFO order runs it after that. See
	// closeBounded: these calls take no context and can block forever.
	closers := []func() error{
		func() error { pool.Close(); return nil },
		kafkaConnection.Close,
	}
	defer func() { closeBounded(log, closers) }()
	if err := pool.Ping(ctx); err != nil {
		// Event history must exist before Kafka publication, so an ingest pod
		// without Postgres cannot serve either API route truthfully.
		return fmt.Errorf("database unreachable: %w", err)
	}

	writer := &kafka.Writer{
		Addr:      kafkaConnection.Addr(),
		Topic:     topic,
		Transport: kafkaConnection.RoundTripper(),
		// Hash on the record key so a tenant always lands on one partition.
		// LeastBytes would spread a tenant across partitions and lose ordering.
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false, // topics come from bootstrap, not by accident
		// The default is 1s waiting for a batch of 100 that a low-rate ingest
		// never fills -- the same default that cost the smoke check a flat
		// second per run. 10ms still batches under load.
		BatchTimeout: 10 * time.Millisecond,
		// Keep the broker wait below ingest's 15-second acceptance budget so
		// Postgres can record the result and release its row lock in time.
		WriteTimeout: 10 * time.Second,
	}
	closers = append(closers, writer.Close)

	server := ingest.New(writer, history.New(pool), topic, log)
	// Safe to be ready immediately: kafka.Writer connects lazily, and an
	// unreachable broker surfaces as a 503 per request rather than at startup.
	server.MarkReady(true)

	srv := &http.Server{Addr: addr, Handler: server.Routes(), ReadHeaderTimeout: 5 * time.Second}
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
		kafkaConnection.Brokers(),
		kafkaConnection.RoundTripper(),
		envOr("RELAY_CONSUMER_GROUP", "relay-deliver"),
		topic,
		lagInterval(log),
		log,
	).Run(ctx)

	log.Info("listening", "addr", addr, "topic", topic, "brokers", kafkaConnection.Brokers(),
		"kafka_auth_mode", kafkaConnection.AuthMode(), "mode", "ingest")
	err = serveUntilShutdown(ctx, log, srv, func() {
		log.Info("shutdown signal received, failing readiness")
		server.MarkReady(false)
		time.Sleep(config.IngestReadinessGrace)
	})
	if err != nil {
		return err
	}
	log.Info("stopped")
	return nil
}

// serveUntilShutdown serves srv and returns only once its graceful shutdown has
// FINISHED -- not when ListenAndServe returns.
//
// Those are different moments, and treating them as one is a bug this code had.
// Shutdown closes the listener first, which makes ListenAndServe return
// ErrServerClosed immediately, and only then waits for in-flight handlers.
// Returning on that signal runs the caller's deferred closes -- the Kafka
// writer, the database pool -- underneath handlers still using them.
//
// relay-ingest makes the consequence concrete rather than theoretical.
// Server.postEvent deliberately detaches from the request context and keeps
// config.IngestAcceptanceTimeout to finish persisting and publishing, so that a
// client hanging up cannot leave an event half-accepted. Closing the pool out
// from under that handler orphans the event anyway, which is the exact outcome
// the detachment was written to prevent.
//
// drain runs after the signal and before the listener closes: it is where
// readiness is failed and the load balancer given time to notice.
func serveUntilShutdown(ctx context.Context, log *slog.Logger, srv *http.Server, drain func()) error {
	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()

	select {
	case err := <-served:
		// The listener failed on its own -- a taken port, most likely. There
		// is nothing to drain.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("server: %w", err)
		}
		return nil
	case <-ctx.Done():
	}

	drain()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), config.IngestServerShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// A handler outlived the budget. Log it rather than swallow it: the
		// caller's deferred closes are about to run regardless, because the
		// pod's grace period ends shortly after this and waiting longer only
		// trades a cut-off request for a SIGKILL.
		log.Error("graceful shutdown did not finish; in-flight requests may be cut off",
			"error", err, "budget", config.IngestServerShutdownTimeout)
	}
	<-served // ErrServerClosed, already sent when the listener closed
	return nil
}

func runDeliver(log *slog.Logger, kafkaConnection *kafkatransport.Connection, schedule config.RetrySchedule, attemptTimeout time.Duration) error {
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
	// One bounded close for every resource, registered before the health
	// server's own shutdown so defer's LIFO order runs it after that. See
	// closeBounded: these calls take no context and can block forever.
	closers := []func() error{
		func() error { pool.Close(); return nil },
		kafkaConnection.Close,
	}
	defer func() { closeBounded(log, closers) }()
	if err := pool.Ping(ctx); err != nil {
		// Unlike the writer, the subscription store is needed for every
		// record, so a database that is not there at startup is fatal rather
		// than a per-record failure.
		return fmt.Errorf("database unreachable: %w", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: kafkaConnection.Brokers(),
		Dialer:  kafkaConnection.Dialer(),
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
	closers = append(closers, reader.Close)

	dlq := &kafka.Writer{
		Addr:                   kafkaConnection.Addr(),
		Topic:                  dlqTopic,
		Transport:              kafkaConnection.RoundTripper(),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchBytes:             delivery.MaxDLQBatchBytes,
		BatchTimeout:           10 * time.Millisecond,
	}
	closers = append(closers, dlq.Close)

	consumer := delivery.NewConsumer(
		reader, dlq, subscriptions.New(pool),
		delivery.NewDeliverer(schedule, attemptTimeout, history.New(pool)), stallBudget, log,
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.HealthShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("consuming", "topic", topic, "dlq", dlqTopic, "group", group,
		"brokers", kafkaConnection.Brokers(), "kafka_auth_mode", kafkaConnection.AuthMode(), "mode", "deliver")
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
// few milliseconds would hammer the broker with four requests a round, and one
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

// Command relay-replay resets relay's inactive consumer group using the same
// Kafka transport configuration as the relay runtime.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/kafkatransport"
	"github.com/lilabrooks/my-local-platform/relay/internal/replay"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "relay replay:", err)
		os.Exit(1)
	}
}

func run() error {
	group := flag.String("group", envOr("RELAY_CONSUMER_GROUP", "relay-deliver"), "consumer group to reset")
	topic := flag.String("topic", envOr("RELAY_TOPIC", "mlp.relay.deliveries"), "topic to replay")
	since := flag.String("since", "earliest", "earliest or an RFC3339 timestamp")
	wait := flag.Duration("wait", 30*time.Second, "maximum time to wait for the group to become inactive")
	flag.Parse()

	connection, err := kafkatransport.New(
		envOr("KAFKA_BOOTSTRAP", "localhost:9092"),
		envOr("KAFKA_AUTH_MODE", kafkatransport.AuthNone),
		os.Getenv("AWS_REGION"),
	)
	if err != nil {
		return fmt.Errorf("kafka transport: %w", err)
	}
	defer func() { _ = connection.Close() }()

	var at *time.Time
	if *since != "earliest" {
		parsed, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return fmt.Errorf("--since must be earliest or RFC3339: %w", err)
		}
		at = &parsed
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := &kafka.Client{
		Addr:      connection.Addr(),
		Transport: connection.RoundTripper(),
		Timeout:   5 * time.Second,
	}
	waitCtx, cancel := context.WithTimeout(ctx, *wait)
	defer cancel()
	if err := replay.WaitInactive(waitCtx, client, connection.Addr(), *group, time.Second); err != nil {
		return err
	}
	results, err := replay.Reset(ctx, client, connection.Addr(), *group, *topic, at)
	if err != nil {
		return err
	}
	for _, result := range results {
		fmt.Printf("partition %-3d -> offset %d\n", result.Partition, result.Offset)
	}
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

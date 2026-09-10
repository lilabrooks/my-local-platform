package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/bootstrap"
	"github.com/lilabrooks/my-local-platform/relay/internal/kafkatransport"
)

const bootstrapTimeout = 2 * time.Minute

func requiredEnvironment(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func run(ctx context.Context) error {
	bootstrapServers, err := requiredEnvironment("KAFKA_BOOTSTRAP")
	if err != nil {
		return err
	}
	authMode, err := requiredEnvironment("KAFKA_AUTH_MODE")
	if err != nil {
		return err
	}
	region, err := requiredEnvironment("AWS_REGION")
	if err != nil {
		return err
	}
	databaseURL, err := requiredEnvironment("DATABASE_URL")
	if err != nil {
		return err
	}
	signingSecret, err := requiredEnvironment("RELAY_SIGNING_SECRET")
	if err != nil {
		return err
	}

	connection, err := kafkatransport.New(ctx, bootstrapServers, authMode, region)
	if err != nil {
		return fmt.Errorf("configure Kafka: %w", err)
	}
	defer func() { _ = connection.Close() }()
	client := &kafka.Client{Addr: connection.Addr(), Transport: connection.RoundTripper()}
	topics, err := bootstrap.EnsureTopics(ctx, client, connection.Addr())
	if err != nil {
		return err
	}
	for _, topic := range topics {
		fmt.Printf("topic %s partitions=%d ready\n", topic.Name, topic.Partitions)
	}

	database, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect to relay database: %w", err)
	}
	defer func() { _ = database.Close(context.Background()) }()
	transaction, err := database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin relay database bootstrap: %w", err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	subscriptions, err := bootstrap.InitializeDatabase(ctx, transaction, signingSecret)
	if err != nil {
		return err
	}
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit relay database bootstrap: %w", err)
	}
	fmt.Printf("relay database ready active_subscriptions=%d\n", subscriptions)
	return nil
}

func redact(message string, sensitive ...string) string {
	for _, value := range sensitive {
		if value != "" {
			message = strings.ReplaceAll(message, value, "[REDACTED]")
		}
	}
	return message
}

func safeError(err error) string {
	databaseURL := os.Getenv("DATABASE_URL")
	password := ""
	if parsed, parseErr := url.Parse(databaseURL); parseErr == nil && parsed.User != nil {
		password, _ = parsed.User.Password()
	}
	return redact(err.Error(), os.Getenv("RELAY_SIGNING_SECRET"), databaseURL, password)
}

func main() {
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(root, bootstrapTimeout)
	defer cancel()
	if err := run(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Fprintln(os.Stderr, "relay bootstrap exceeded 2 minutes")
		} else {
			fmt.Fprintln(os.Stderr, safeError(err))
		}
		os.Exit(1)
	}
}

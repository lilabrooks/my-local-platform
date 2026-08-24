package checks

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

// Kafka produces one message and consumes it back from the same topic.
func Kafka(cfg platform.Config) Check {
	return Check{Name: "kafka", Run: func(ctx context.Context) (string, error) {
		brokers := strings.Split(cfg.KafkaBrokers, ",")
		marker := fmt.Sprintf("smoke-%d", time.Now().UnixNano())

		w := &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  cfg.KafkaTopic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: false, // topics come from bootstrap, not by accident
			RequiredAcks:           kafka.RequireAll,
		}
		defer func() { _ = w.Close() }()

		if err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte("smoke"),
			Value: []byte(marker),
		}); err != nil {
			return "", fmt.Errorf("produce: %w", err)
		}

		// A fresh group id each run, starting at the earliest offset, so the
		// check does not depend on committed offsets from previous runs.
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       cfg.KafkaTopic,
			GroupID:     fmt.Sprintf("smoke-%d", time.Now().UnixNano()),
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
		})
		defer func() { _ = r.Close() }()

		deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for {
			m, err := r.ReadMessage(deadline)
			if err != nil {
				return "", fmt.Errorf("consume %s: %w", marker, err)
			}
			if string(m.Value) == marker {
				return fmt.Sprintf("%s partition %d offset %d", cfg.KafkaTopic, m.Partition, m.Offset), nil
			}
		}
	}}
}

// RabbitMQ declares a queue, publishes, and consumes the message back.
func RabbitMQ(cfg platform.Config) Check {
	return Check{Name: "rabbitmq", Run: func(ctx context.Context) (string, error) {
		conn, err := amqp.Dial(cfg.RabbitURL)
		if err != nil {
			return "", fmt.Errorf("dial: %w", err)
		}
		defer func() { _ = conn.Close() }()

		ch, err := conn.Channel()
		if err != nil {
			return "", fmt.Errorf("channel: %w", err)
		}
		defer func() { _ = ch.Close() }()

		// Durable so the queue survives a broker restart, matching how a real
		// service would declare it.
		q, err := ch.QueueDeclare("mlp.smoke", true, false, false, false, nil)
		if err != nil {
			return "", fmt.Errorf("declare queue: %w", err)
		}

		marker := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
		if err := ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(marker),
			DeliveryMode: amqp.Persistent,
		}); err != nil {
			return "", fmt.Errorf("publish: %w", err)
		}

		deliveries, err := ch.Consume(q.Name, "", true, false, false, false, nil)
		if err != nil {
			return "", fmt.Errorf("consume: %w", err)
		}

		timeout := time.After(20 * time.Second)
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					return "", errors.New("delivery channel closed before the message arrived")
				}
				if string(d.Body) == marker {
					return "queue " + q.Name + " round trip", nil
				}
			case <-timeout:
				return "", fmt.Errorf("message %s did not arrive within 20s", marker)
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}}
}

// Postgres creates a table, writes a row, and reads it back.
func Postgres(cfg platform.Config) Check {
	return Check{Name: "postgres", Run: func(ctx context.Context) (string, error) {
		conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			return "", fmt.Errorf("connect: %w", err)
		}
		defer func() { _ = conn.Close(ctx) }()

		if _, err := conn.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS smoke_check (
				id         bigserial PRIMARY KEY,
				marker     text        NOT NULL,
				created_at timestamptz NOT NULL DEFAULT now()
			)`); err != nil {
			return "", fmt.Errorf("create table: %w", err)
		}

		marker := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
		var id int64
		if err := conn.QueryRow(ctx,
			`INSERT INTO smoke_check (marker) VALUES ($1) RETURNING id`, marker,
		).Scan(&id); err != nil {
			return "", fmt.Errorf("insert: %w", err)
		}

		var got string
		if err := conn.QueryRow(ctx,
			`SELECT marker FROM smoke_check WHERE id = $1`, id,
		).Scan(&got); err != nil {
			return "", fmt.Errorf("select: %w", err)
		}
		if got != marker {
			return "", fmt.Errorf("round trip mismatch: wrote %q, read %q", marker, got)
		}

		var version string
		_ = conn.QueryRow(ctx, `SHOW server_version`).Scan(&version)
		return fmt.Sprintf("row %d on postgres %s", id, version), nil
	}}
}

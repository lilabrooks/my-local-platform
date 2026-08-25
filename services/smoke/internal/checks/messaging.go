package checks

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

// Kafka produces one message and reads that exact record back.
//
// The produce response carries the partition and offset Kafka assigned, so the
// reader is pointed straight at the record. Cost is one fetch no matter how
// large the topic has grown.
//
// The earlier version joined a fresh consumer group at kafka.FirstOffset and
// scanned forward for its own marker, so consume time grew with the topic: it
// passed in ~10s on a clean topic and timed out at ~31s once mlp.events held
// 60,001 messages. See issue #1.
func Kafka(cfg platform.Config) Check {
	return Check{Name: "kafka", Run: func(ctx context.Context) (string, error) {
		brokers := strings.Split(cfg.KafkaBrokers, ",")
		marker := fmt.Sprintf("smoke-%d", time.Now().UnixNano())

		// Completion runs on a writer goroutine, but with Async left false
		// WriteMessages blocks until it has returned, so these are safe to
		// read once the write succeeds. The mutex is what makes that explicit
		// to the race detector.
		var (
			mu        sync.Mutex
			partition int
			offset    int64
			acked     bool
		)

		w := &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  cfg.KafkaTopic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: false, // topics come from bootstrap, not by accident
			// RequireAll is also what makes the assigned offset available:
			// with RequireNone the broker sends no produce response to read
			// it from, and this check would have nowhere to seek to.
			RequiredAcks: kafka.RequireAll,
			// This check writes exactly one message, so the default batch of
			// 100 never fills and every run pays BatchTimeout -- a flat second
			// of waiting for a batch that cannot arrive.
			BatchSize: 1,
			Completion: func(msgs []kafka.Message, err error) {
				if err != nil || len(msgs) == 0 {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				partition, offset, acked = msgs[0].Partition, msgs[0].Offset, true
			},
		}
		defer func() { _ = w.Close() }()

		if err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte("smoke"),
			Value: []byte(marker),
		}); err != nil {
			return "", fmt.Errorf("produce: %w", err)
		}

		mu.Lock()
		wrotePartition, wroteOffset, ok := partition, offset, acked
		mu.Unlock()
		if !ok {
			return "", fmt.Errorf("produce %s: no partition or offset in the produce response", marker)
		}

		// No consumer group. One known record on one known partition needs no
		// group coordination, and nothing carries over between runs.
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     cfg.KafkaTopic,
			Partition: wrotePartition,
			MinBytes:  1,
			MaxBytes:  10e6,
			// Close blocks on whatever fetch is in flight, and once this check
			// has its record the next fetch has nothing to return -- so it sits
			// out the full MaxWait, which defaults to 10s. That wait was most
			// of this check's runtime and none of its work.
			MaxWait: 250 * time.Millisecond,
		})
		defer func() { _ = r.Close() }()

		if err := r.SetOffset(wroteOffset); err != nil {
			return "", fmt.Errorf("seek to partition %d offset %d: %w", wrotePartition, wroteOffset, err)
		}

		// The runner already bounds every check at checkTimeout; a second
		// deadline here would just be another number to keep in agreement.
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return "", fmt.Errorf("consume %s: %w", marker, err)
		}
		if string(m.Value) != marker {
			return "", fmt.Errorf("read back %q at partition %d offset %d, want %q",
				m.Value, m.Partition, m.Offset, marker)
		}

		return fmt.Sprintf("%s partition %d offset %d", cfg.KafkaTopic, m.Partition, m.Offset), nil
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

// Command relay-capture performs bounded operator observations inside the VPC.
// It never joins a consumer group. Replay keeps its separate deliver identity.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/kafkatransport"
)

const deliveries = "mlp.relay.deliveries"
const dlq = "mlp.relay.deliveries.dlq"

type record struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Key       []byte    `json:"key"`
	Value     []byte    `json:"value"`
}

func captured(m kafka.Message) record {
	return record{m.Topic, m.Partition, m.Offset, m.Time, m.Key, m.Value}
}

func validateInput(operation, topic string, starts map[int]int64) error {
	if topic != deliveries && topic != dlq {
		return errors.New("unsupported proof topic")
	}
	switch operation {
	case "snapshot", "poison", "database":
	case "read":
		want := 12
		if topic == dlq {
			want = 1
		}
		if len(starts) != want {
			return errors.New("incomplete partition start snapshot")
		}
		for p := 0; p < want; p++ {
			if value, ok := starts[p]; !ok || value < 0 {
				return errors.New("invalid partition start snapshot")
			}
		}
	default:
		return errors.New("unsupported capture operation")
	}
	return nil
}

func offsets(ctx context.Context, c *kafkatransport.Connection, topic string) (map[int]int64, error) {
	d := c.Dialer()
	if d == nil {
		d = &kafka.Dialer{Timeout: 10 * time.Second}
	}
	conn, err := d.DialContext(ctx, "tcp", c.Brokers()[0])
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, err
	}
	result := map[int]int64{}
	for _, p := range partitions {
		leader, err := d.DialLeader(ctx, "tcp", c.Brokers()[0], topic, p.ID)
		if err != nil {
			return nil, err
		}
		if err := leader.SetDeadline(deadline); err != nil {
			_ = leader.Close()
			return nil, err
		}
		end, err := leader.ReadLastOffset()
		_ = leader.Close()
		if err != nil {
			return nil, err
		}
		result[p.ID] = end
	}
	if err := validateInput("read", topic, result); err != nil {
		return nil, err
	}
	return result, nil
}

func readSnapshot(ctx context.Context, c *kafkatransport.Connection, topic string, starts, ends map[int]int64) ([]record, error) {
	result := []record{}
	for p, start := range starts {
		end, ok := ends[p]
		if !ok || end < start || end-start > 2000 {
			return nil, errors.New("invalid or oversized proof interval")
		}
		if start == end {
			continue
		}
		r := kafka.NewReader(kafka.ReaderConfig{Brokers: c.Brokers(), Dialer: c.Dialer(), Topic: topic, Partition: p, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 250 * time.Millisecond})
		if err := r.SetOffset(start); err != nil {
			_ = r.Close()
			return nil, err
		}
		for next := start; next < end; {
			m, err := r.ReadMessage(ctx)
			if err != nil {
				_ = r.Close()
				return nil, err
			}
			if m.Offset < next || m.Offset >= end {
				_ = r.Close()
				return nil, errors.New("unexpected offset inside proof interval")
			}
			result = append(result, captured(m))
			next = m.Offset + 1
		}
		_ = r.Close()
	}
	return result, nil
}

func run(ctx context.Context) (any, error) {
	operation := flag.String("operation", "snapshot", "snapshot, read, poison, database")
	topic := flag.String("topic", deliveries, "one of the two proof topics")
	startJSON := flag.String("starts", "{}", "partition offsets from snapshot")
	eventID := flag.String("event-id", "", "accepted proof event")
	marker := flag.String("marker", "", "unique non-secret cohort marker")
	flag.Parse()
	var starts map[int]int64
	if err := json.Unmarshal([]byte(*startJSON), &starts); err != nil {
		return nil, err
	}
	if err := validateInput(*operation, *topic, starts); err != nil {
		return nil, err
	}
	if *operation == "database" {
		if !regexp.MustCompile(`^evt_[0-9a-f]{32}$`).MatchString(*eventID) || *marker == "" {
			return nil, errors.New("invalid event identity")
		}
		conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
		if err != nil {
			return nil, errors.New("database connection failed")
		}
		defer func() { _ = conn.Close(ctx) }()
		var count int
		err = conn.QueryRow(ctx, `SELECT count(*) FROM relay_events WHERE id=$1 AND tenant_id='acme' AND idempotency_key=$2 AND published_at IS NOT NULL AND idempotency_claimed_at IS NOT NULL`, *eventID, *marker).Scan(&count)
		if err != nil {
			return nil, errors.New("database proof query failed")
		}
		if count != 1 {
			return nil, errors.New("expected one persisted published idempotency claim")
		}
		return map[string]any{"event_id": *eventID, "published_claims": count}, nil
	}
	c, err := kafkatransport.New(ctx, os.Getenv("KAFKA_BOOTSTRAP"), os.Getenv("KAFKA_AUTH_MODE"), os.Getenv("AWS_REGION"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close() }()
	if *operation == "poison" {
		if !regexp.MustCompile(`^[a-zA-Z0-9-]{1,80}$`).MatchString(*marker) {
			return nil, errors.New("invalid poison marker")
		}
		m := kafka.Message{Topic: deliveries, Key: []byte(*marker), Value: []byte("{invalid-" + *marker), Time: time.Now().UTC().Truncate(time.Millisecond)}
		var mu sync.Mutex
		var acked bool
		var observed kafka.Message
		writer := &kafka.Writer{Addr: c.Addr(), Transport: c.RoundTripper(), Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchSize: 1,
			Completion: func(messages []kafka.Message, err error) {
				mu.Lock()
				defer mu.Unlock()
				if err == nil && len(messages) == 1 {
					observed = messages[0]
					acked = true
				}
			}}
		defer func() { _ = writer.Close() }()
		if err := writer.WriteMessages(ctx, m); err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		if !acked {
			return nil, errors.New("missing producer acknowledgement")
		}
		return captured(observed), nil
	}
	ends, err := offsets(ctx, c, *topic)
	if err != nil {
		return nil, err
	}
	if *operation == "snapshot" {
		return ends, nil
	}
	records, err := readSnapshot(ctx, c, *topic, starts, ends)
	return map[string]any{"starts": starts, "ends": ends, "records": records}, err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	result, err := run(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "capture operation failed; inspect the operation and non-secret runtime state")
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		os.Exit(1)
	}
}

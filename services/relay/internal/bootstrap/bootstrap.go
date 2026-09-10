// Package bootstrap owns the relay broker topology and database initialization
// shared by local development and the short live-AWS bootstrap Job.
package bootstrap

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

const metadataAttempts = 5

//go:embed topics.txt
var topicText string

//go:embed schema.sql
var SchemaSQL string

type Topic struct {
	Name       string
	Partitions int
}

func Topics() ([]Topic, error) {
	lines := strings.Split(strings.TrimSpace(topicText), "\n")
	topics := make([]Topic, 0, len(lines))
	seen := make(map[string]bool, len(lines))
	for index, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("topics.txt line %d must contain name and partition count", index+1)
		}
		partitions, err := strconv.Atoi(fields[1])
		if err != nil || partitions < 1 {
			return nil, fmt.Errorf("topics.txt line %d has invalid partition count %q", index+1, fields[1])
		}
		if seen[fields[0]] {
			return nil, fmt.Errorf("topics.txt repeats %q", fields[0])
		}
		seen[fields[0]] = true
		topics = append(topics, Topic{Name: fields[0], Partitions: partitions})
	}
	if len(topics) == 0 {
		return nil, errors.New("topics.txt is empty")
	}
	return topics, nil
}

type KafkaAdmin interface {
	CreateTopics(context.Context, *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
}

func EnsureTopics(ctx context.Context, client KafkaAdmin, address net.Addr) ([]Topic, error) {
	return ensureTopics(ctx, client, address, time.Second)
}

func ensureTopics(ctx context.Context, client KafkaAdmin, address net.Addr, retryDelay time.Duration) ([]Topic, error) {
	topics, err := Topics()
	if err != nil {
		return nil, err
	}
	configs := make([]kafka.TopicConfig, 0, len(topics))
	names := make([]string, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: -1,
		})
		names = append(names, topic.Name)
	}
	created, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Addr: address, Topics: configs})
	if err != nil {
		return nil, fmt.Errorf("create relay topics: %w", err)
	}
	if created == nil {
		return nil, errors.New("create relay topics returned no response")
	}
	for _, topic := range topics {
		if topicErr := created.Errors[topic.Name]; topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
			return nil, fmt.Errorf("create topic %s: %w", topic.Name, topicErr)
		}
	}
	var metadataErr error
	for attempt := 1; attempt <= metadataAttempts; attempt++ {
		metadata, err := client.Metadata(ctx, &kafka.MetadataRequest{Addr: address, Topics: names})
		if err != nil {
			metadataErr = fmt.Errorf("read relay topic metadata: %w", err)
		} else if metadata == nil {
			metadataErr = errors.New("read relay topic metadata returned no response")
		} else {
			metadataErr = verifyTopicMetadata(topics, metadata)
		}
		if metadataErr == nil {
			return topics, nil
		}
		if attempt == metadataAttempts {
			break
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("verify relay topic metadata: %w", ctx.Err())
		case <-timer.C:
		}
	}
	return nil, metadataErr
}

func verifyTopicMetadata(topics []Topic, metadata *kafka.MetadataResponse) error {
	actual := make(map[string]kafka.Topic, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		actual[topic.Name] = topic
	}
	for _, expected := range topics {
		got, found := actual[expected.Name]
		if !found {
			return fmt.Errorf("topic %s missing after creation", expected.Name)
		}
		if got.Error != nil {
			return fmt.Errorf("describe topic %s: %w", expected.Name, got.Error)
		}
		if len(got.Partitions) != expected.Partitions {
			return fmt.Errorf("topic %s has %d partitions, want %d", expected.Name, len(got.Partitions), expected.Partitions)
		}
	}
	return nil
}

type SQLExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func InitializeDatabase(ctx context.Context, tx SQLExecutor, signingSecret string) (int, error) {
	if signingSecret == "" {
		return 0, errors.New("relay signing secret is empty")
	}
	if _, err := tx.Exec(ctx, "SELECT set_config('mlp.signing_secret', $1, true)", signingSecret); err != nil {
		return 0, fmt.Errorf("stage signing secret in database transaction: %w", err)
	}
	if _, err := tx.Exec(ctx, SchemaSQL, pgx.QueryExecModeSimpleProtocol); err != nil {
		return 0, fmt.Errorf("apply relay schema and subscriptions: %w", err)
	}
	var subscriptions int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM relay_subscriptions WHERE active").Scan(&subscriptions); err != nil {
		return 0, fmt.Errorf("count active relay subscriptions: %w", err)
	}
	return subscriptions, nil
}

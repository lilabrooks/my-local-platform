package bootstrap

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

type fakeKafkaAdmin struct {
	createRequest *kafka.CreateTopicsRequest
	create        *kafka.CreateTopicsResponse
	createErr     error
	metadata      *kafka.MetadataResponse
	metadataQueue []*kafka.MetadataResponse
	metadataErr   error
	metadataCalls int
}

func (f *fakeKafkaAdmin) CreateTopics(_ context.Context, request *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error) {
	f.createRequest = request
	return f.create, f.createErr
}

func (f *fakeKafkaAdmin) Metadata(_ context.Context, _ *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	f.metadataCalls++
	if len(f.metadataQueue) != 0 {
		metadata := f.metadataQueue[0]
		f.metadataQueue = f.metadataQueue[1:]
		return metadata, nil
	}
	return f.metadata, f.metadataErr
}

func metadataTopic(name string, partitions int) kafka.Topic {
	return kafka.Topic{Name: name, Partitions: make([]kafka.Partition, partitions)}
}

func TestEnsureTopicsCreatesAndChecksTheSharedTopology(t *testing.T) {
	admin := &fakeKafkaAdmin{
		create: &kafka.CreateTopicsResponse{Errors: map[string]error{
			"mlp.relay.deliveries":     kafka.TopicAlreadyExists,
			"mlp.relay.deliveries.dlq": nil,
		}},
		metadata: &kafka.MetadataResponse{Topics: []kafka.Topic{
			metadataTopic("mlp.relay.deliveries", 12),
			metadataTopic("mlp.relay.deliveries.dlq", 1),
		}},
	}

	topics, err := EnsureTopics(context.Background(), admin, kafka.TCP("broker:9098"))
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 || topics[0].Partitions != 12 || topics[1].Partitions != 1 {
		t.Fatalf("topics = %#v", topics)
	}
	if got := admin.createRequest.Topics[0].ReplicationFactor; got != -1 {
		t.Fatalf("replication factor = %d, want broker default -1", got)
	}
}

func TestEnsureTopicsRejectsTopologyDrift(t *testing.T) {
	admin := &fakeKafkaAdmin{
		create: &kafka.CreateTopicsResponse{Errors: map[string]error{}},
		metadata: &kafka.MetadataResponse{Topics: []kafka.Topic{
			metadataTopic("mlp.relay.deliveries", 3),
			metadataTopic("mlp.relay.deliveries.dlq", 1),
		}},
	}

	_, err := ensureTopics(context.Background(), admin, kafka.TCP("broker:9098"), 0)
	if err == nil || !strings.Contains(err.Error(), "has 3 partitions, want 12") {
		t.Fatalf("error = %v", err)
	}
	if admin.metadataCalls != metadataAttempts {
		t.Fatalf("metadata calls = %d, want %d", admin.metadataCalls, metadataAttempts)
	}
}

func TestEnsureTopicsRetriesMetadataPropagation(t *testing.T) {
	admin := &fakeKafkaAdmin{
		create: &kafka.CreateTopicsResponse{Errors: map[string]error{}},
		metadataQueue: []*kafka.MetadataResponse{
			{Topics: []kafka.Topic{
				metadataTopic("mlp.relay.deliveries", 3),
				metadataTopic("mlp.relay.deliveries.dlq", 1),
			}},
			{Topics: []kafka.Topic{
				metadataTopic("mlp.relay.deliveries", 12),
				metadataTopic("mlp.relay.deliveries.dlq", 1),
			}},
		},
	}

	if _, err := ensureTopics(context.Background(), admin, kafka.TCP("broker:9098"), 0); err != nil {
		t.Fatal(err)
	}
	if admin.metadataCalls != 2 {
		t.Fatalf("metadata calls = %d, want 2", admin.metadataCalls)
	}
}

func TestEnsureTopicsPropagatesCreateFailure(t *testing.T) {
	want := errors.New("denied")
	admin := &fakeKafkaAdmin{createErr: want}
	_, err := EnsureTopics(context.Background(), admin, kafka.TCP("broker:9098"))
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

type fakeRow struct {
	count int
	err   error
}

func (r fakeRow) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	*(destinations[0].(*int)) = r.count
	return nil
}

type fakeSQL struct {
	queries [][]any
	row     fakeRow
}

func (f *fakeSQL) Exec(_ context.Context, query string, arguments ...any) (pgconn.CommandTag, error) {
	entry := []any{query}
	entry = append(entry, arguments...)
	f.queries = append(f.queries, entry)
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func (f *fakeSQL) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return f.row
}

func TestInitializeDatabaseKeepsSecretOutOfSchemaText(t *testing.T) {
	fake := &fakeSQL{row: fakeRow{count: 19}}
	secret := "private-test-signing-value"

	count, err := InitializeDatabase(context.Background(), fake, secret)
	if err != nil {
		t.Fatal(err)
	}
	if count != 19 {
		t.Fatalf("count = %d, want 19", count)
	}
	if len(fake.queries) != 2 {
		t.Fatalf("queries = %d, want 2", len(fake.queries))
	}
	if fake.queries[0][1] != secret {
		t.Fatal("secret was not passed as a bound parameter")
	}
	if strings.Contains(SchemaSQL, secret) || !strings.Contains(SchemaSQL, "current_setting('mlp.signing_secret')") {
		t.Fatal("schema does not read the transaction-local signing secret")
	}
}

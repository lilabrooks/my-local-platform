package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/segmentio/kafka-go"
)

func TestLagFor(t *testing.T) {
	tests := []struct {
		name                   string
		committed, first, last int64
		want                   int64
	}{
		{"caught up", 100, 0, 100, 0},
		{"behind by ten", 90, 0, 100, 10},
		{"empty partition", 0, 0, 0, 0},
		{
			// The case worth the test. A group that has never committed
			// reports -1, and naive arithmetic gives last-(-1) = last+1, which
			// overstates by one on every partition -- twelve phantom records
			// on the demo's headline number.
			name:      "never committed counts the whole retained log",
			committed: -1, first: 0, last: 500, want: 500,
		},
		{
			// Same, on a partition whose head has been aged out by retention.
			// The consumer will start at the first surviving offset, not at 0,
			// so lag is what it will actually read.
			name:      "never committed after retention has trimmed the head",
			committed: -1, first: 200, last: 500, want: 300,
		},
		{
			// Not arithmetic to publish as a negative gauge: it means the
			// partition was truncated or the topic recreated under the group.
			name:      "committed ahead of the high watermark clamps to zero",
			committed: 600, first: 0, last: 500, want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lagFor(tt.committed, tt.first, tt.last); got != tt.want {
				t.Errorf("lagFor(committed=%d, first=%d, last=%d) = %d, want %d",
					tt.committed, tt.first, tt.last, got, tt.want)
			}
		})
	}
}

// fakeBroker answers the three requests the poller makes.
type fakeBroker struct {
	partitions []int
	committed  map[int]int64
	bounds     map[int]offsetBounds

	metadataErr error
	fetchErr    error
	listErr     error

	// partitionErr marks one partition as failing in both offset responses,
	// which is how a broker reports a partition without a leader.
	partitionErr map[int]error
}

func (f *fakeBroker) Metadata(_ context.Context, _ *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	if f.metadataErr != nil {
		return nil, f.metadataErr
	}
	parts := make([]kafka.Partition, 0, len(f.partitions))
	for _, id := range f.partitions {
		parts = append(parts, kafka.Partition{ID: id, Topic: testTopic})
	}
	return &kafka.MetadataResponse{
		Topics: []kafka.Topic{{Name: testTopic, Partitions: parts}},
	}, nil
}

func (f *fakeBroker) OffsetFetch(_ context.Context, _ *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error) {
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	out := make([]kafka.OffsetFetchPartition, 0, len(f.partitions))
	for _, id := range f.partitions {
		p := kafka.OffsetFetchPartition{Partition: id, CommittedOffset: f.committed[id]}
		p.Error = f.partitionErr[id]
		out = append(out, p)
	}
	return &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{testTopic: out}}, nil
}

func (f *fakeBroker) ListOffsets(_ context.Context, _ *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]kafka.PartitionOffsets, 0, len(f.partitions))
	for _, id := range f.partitions {
		b := f.bounds[id]
		p := kafka.PartitionOffsets{Partition: id, FirstOffset: b.first, LastOffset: b.last}
		p.Error = f.partitionErr[id]
		out = append(out, p)
	}
	return &kafka.ListOffsetsResponse{Topics: map[string][]kafka.PartitionOffsets{testTopic: out}}, nil
}

const (
	testTopic = "mlp.relay.deliveries"
	testGroup = "relay-deliver"
)

func quietPoller(c offsetClient) *LagPoller {
	return newLagPoller(c, testGroup, testTopic, 0,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func resetLagGauges() {
	ConsumerLag.Reset()
	ConsumerLagTotal.Reset()
	CommittedOffset.Reset()
	HighWatermark.Reset()
}

func TestRefreshPublishesPerPartitionAndTotalLag(t *testing.T) {
	resetLagGauges()

	broker := &fakeBroker{
		partitions: []int{0, 1, 2},
		committed:  map[int]int64{0: 90, 1: 100, 2: -1},
		bounds: map[int]offsetBounds{
			0: {first: 0, last: 100}, // 10 behind
			1: {first: 0, last: 100}, // caught up
			2: {first: 5, last: 30},  // never committed: 25 to read
		},
	}

	if err := quietPoller(broker).Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	for partition, want := range map[string]float64{"0": 10, "1": 0, "2": 25} {
		got := testutil.ToFloat64(ConsumerLag.WithLabelValues(testGroup, testTopic, partition))
		if got != want {
			t.Errorf("lag on partition %s = %v, want %v", partition, got, want)
		}
	}

	// The total is what the demo panel plots, so it has to be the sum of the
	// parts and not, say, the last partition seen.
	if got := testutil.ToFloat64(ConsumerLagTotal.WithLabelValues(testGroup, testTopic)); got != 35 {
		t.Errorf("total lag = %v, want 35", got)
	}
	if got := testutil.ToFloat64(HighWatermark.WithLabelValues(testTopic, "2")); got != 30 {
		t.Errorf("high watermark on partition 2 = %v, want 30", got)
	}
	if got := testutil.ToFloat64(LagRefreshedAt); got == 0 {
		t.Error("refresh timestamp was not set, so a stale scrape would be indistinguishable from a fresh one")
	}
}

func TestRefreshRecomputesTotalRatherThanAccumulating(t *testing.T) {
	resetLagGauges()

	broker := &fakeBroker{
		partitions: []int{0},
		committed:  map[int]int64{0: 0},
		bounds:     map[int]offsetBounds{0: {first: 0, last: 50}},
	}
	poller := quietPoller(broker)

	for range 3 {
		if err := poller.Refresh(context.Background()); err != nil {
			t.Fatalf("Refresh: %v", err)
		}
	}

	// A gauge that added rather than set would read 150 here, and the demo
	// would show lag climbing while the consumer sat perfectly still.
	if got := testutil.ToFloat64(ConsumerLagTotal.WithLabelValues(testGroup, testTopic)); got != 50 {
		t.Errorf("total lag after three refreshes = %v, want 50", got)
	}
}

func TestRefreshErrorsRatherThanReportingZeroLag(t *testing.T) {
	// Each of these would otherwise leave the gauges untouched at zero, which
	// on a panel is indistinguishable from a consumer that has caught up.
	tests := map[string]*fakeBroker{
		"metadata unavailable": {metadataErr: errors.New("broker unreachable")},
		"offset fetch failed": {
			partitions: []int{0},
			fetchErr:   errors.New("coordinator not available"),
		},
		"list offsets failed": {
			partitions: []int{0},
			committed:  map[int]int64{0: 0},
			listErr:    errors.New("not leader for partition"),
		},
		"topic has no partitions": {partitions: nil},
	}

	for name, broker := range tests {
		t.Run(name, func(t *testing.T) {
			if err := quietPoller(broker).Refresh(context.Background()); err == nil {
				t.Fatal("Refresh returned nil; a failed poll must be reported, not published as zero lag")
			}
		})
	}
}

func TestRefreshKeepsGoodPartitionsWhenOneFails(t *testing.T) {
	resetLagGauges()

	broker := &fakeBroker{
		partitions:   []int{0, 1},
		committed:    map[int]int64{0: 40, 1: 0},
		bounds:       map[int]offsetBounds{0: {first: 0, last: 100}, 1: {first: 0, last: 999}},
		partitionErr: map[int]error{1: errors.New("leader not available")},
	}

	if err := quietPoller(broker).Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := testutil.ToFloat64(ConsumerLag.WithLabelValues(testGroup, testTopic, "0")); got != 60 {
		t.Errorf("lag on the healthy partition = %v, want 60", got)
	}
	// The failing partition contributes nothing rather than a made-up number.
	if got := testutil.ToFloat64(ConsumerLagTotal.WithLabelValues(testGroup, testTopic)); got != 60 {
		t.Errorf("total lag = %v, want 60 from the one readable partition", got)
	}
}

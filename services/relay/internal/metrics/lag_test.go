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

// fakeBroker answers the four requests the poller makes.
type fakeBroker struct {
	partitions []int
	committed  map[int]int64
	bounds     map[int]offsetBounds
	members    []kafka.DescribeGroupsResponseMember
	groupState string

	metadataErr error
	fetchErr    error
	listErr     error
	describeErr error
	groupErr    error
	omitGroup   bool

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

func (f *fakeBroker) DescribeGroups(_ context.Context, _ *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	if f.omitGroup {
		return &kafka.DescribeGroupsResponse{}, nil
	}
	state := f.groupState
	if state == "" {
		state = "Stable"
	}
	return &kafka.DescribeGroupsResponse{Groups: []kafka.DescribeGroupsResponseGroup{{
		GroupID:    testGroup,
		GroupState: state,
		Error:      f.groupErr,
		Members:    f.members,
	}}}, nil
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
	LagPartitionsMissing.Reset()
	GroupMembers.Reset()
	GroupUnassignedMembers.Reset()
	TopicPartitionsUnassigned.Reset()
	LagRefreshedAt.Set(0)
}

func TestRefreshPublishesGroupAssignmentCoverage(t *testing.T) {
	resetLagGauges()

	broker := &fakeBroker{
		partitions: []int{0, 1, 2, 3},
		committed:  map[int]int64{0: 0, 1: 0, 2: 0, 3: 0},
		bounds: map[int]offsetBounds{
			0: {}, 1: {}, 2: {}, 3: {},
		},
		members: []kafka.DescribeGroupsResponseMember{
			{
				MemberID: "member-a",
				MemberAssignments: kafka.DescribeGroupsResponseAssignments{Topics: []kafka.GroupMemberTopic{{
					Topic: testTopic, Partitions: []int{0, 1},
				}}},
			},
			{
				MemberID: "member-b",
				MemberAssignments: kafka.DescribeGroupsResponseAssignments{Topics: []kafka.GroupMemberTopic{{
					Topic: testTopic, Partitions: []int{2},
				}}},
			},
			{MemberID: "member-c"},
		},
	}

	if err := quietPoller(broker).Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"group members", testutil.ToFloat64(GroupMembers.WithLabelValues(testGroup)), 3},
		{"members with zero partitions", testutil.ToFloat64(GroupUnassignedMembers.WithLabelValues(testGroup)), 1},
		{"topic partitions without an owner", testutil.ToFloat64(TopicPartitionsUnassigned.WithLabelValues(testGroup, testTopic)), 1},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
}

func TestRefreshPublishesAnEmptyGroupAsMeasuredZero(t *testing.T) {
	for _, state := range []string{"Empty", "Dead"} {
		t.Run(state, func(t *testing.T) {
			resetLagGauges()

			broker := &fakeBroker{
				partitions: []int{0, 1},
				committed:  map[int]int64{0: 0, 1: 0},
				bounds:     map[int]offsetBounds{0: {}, 1: {}},
				groupState: state,
			}
			if err := quietPoller(broker).Refresh(context.Background()); err != nil {
				t.Fatalf("Refresh: %v", err)
			}

			if got := testutil.ToFloat64(GroupMembers.WithLabelValues(testGroup)); got != 0 {
				t.Errorf("group members = %v, want measured zero", got)
			}
			if got := testutil.ToFloat64(GroupUnassignedMembers.WithLabelValues(testGroup)); got != 0 {
				t.Errorf("unassigned members = %v, want measured zero", got)
			}
			if got := testutil.ToFloat64(TopicPartitionsUnassigned.WithLabelValues(testGroup, testTopic)); got != 2 {
				t.Errorf("unassigned partitions = %v, want 2", got)
			}
			if got := testutil.ToFloat64(LagRefreshedAt); got == 0 {
				t.Error("measured zero group evidence did not receive a freshness timestamp")
			}
		})
	}
}

func TestGroupCoverageFailureKeepsTheLastCompleteMeasurement(t *testing.T) {
	tests := map[string]func(*fakeBroker){
		"request error": func(f *fakeBroker) {
			f.describeErr = errors.New("coordinator unavailable")
		},
		"group error": func(f *fakeBroker) {
			f.groupErr = errors.New("group authorization failed")
		},
		"group missing from response": func(f *fakeBroker) {
			f.omitGroup = true
		},
		"preparing rebalance": func(f *fakeBroker) {
			f.groupState = "PreparingRebalance"
		},
		"completing rebalance": func(f *fakeBroker) {
			f.groupState = "CompletingRebalance"
		},
		"duplicate owner": func(f *fakeBroker) {
			f.members = append(f.members, kafka.DescribeGroupsResponseMember{
				MemberID: "member-b",
				MemberAssignments: kafka.DescribeGroupsResponseAssignments{Topics: []kafka.GroupMemberTopic{{
					Topic: testTopic, Partitions: []int{0},
				}}},
			})
		},
		"unknown partition": func(f *fakeBroker) {
			f.members[0].MemberAssignments.Topics[0].Partitions = []int{0, 9}
		},
	}

	for name, breakPoll := range tests {
		t.Run(name, func(t *testing.T) {
			resetLagGauges()
			healthy := &fakeBroker{
				partitions: []int{0},
				committed:  map[int]int64{0: 0},
				bounds:     map[int]offsetBounds{0: {}},
				members: []kafka.DescribeGroupsResponseMember{{
					MemberID: "member-a",
					MemberAssignments: kafka.DescribeGroupsResponseAssignments{Topics: []kafka.GroupMemberTopic{{
						Topic: testTopic, Partitions: []int{0},
					}}},
				}},
			}
			if err := quietPoller(healthy).Refresh(context.Background()); err != nil {
				t.Fatalf("healthy Refresh: %v", err)
			}
			LagRefreshedAt.Set(123)

			failed := *healthy
			failed.members = append([]kafka.DescribeGroupsResponseMember(nil), healthy.members...)
			failed.members[0].MemberAssignments.Topics = append(
				[]kafka.GroupMemberTopic(nil), healthy.members[0].MemberAssignments.Topics...)
			breakPoll(&failed)

			if err := quietPoller(&failed).Refresh(context.Background()); err == nil {
				t.Fatal("Refresh returned nil after incomplete group evidence")
			}
			if got := testutil.ToFloat64(GroupMembers.WithLabelValues(testGroup)); got != 1 {
				t.Errorf("group members after failed refresh = %v, want last complete value 1", got)
			}
			if got := testutil.ToFloat64(GroupUnassignedMembers.WithLabelValues(testGroup)); got != 0 {
				t.Errorf("unassigned members after failed refresh = %v, want last complete value 0", got)
			}
			if got := testutil.ToFloat64(TopicPartitionsUnassigned.WithLabelValues(testGroup, testTopic)); got != 0 {
				t.Errorf("unassigned partitions after failed refresh = %v, want last complete value 0", got)
			}
			if got := testutil.ToFloat64(LagRefreshedAt); got != 123 {
				t.Errorf("refresh timestamp after incomplete group evidence = %v, want 123", got)
			}
		})
	}
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

// A partial read must not publish a partial TOTAL. Summing only the partitions
// that answered produces a smaller number, and stamping it fresh makes it
// indistinguishable on the panel from a backlog that drained. KEDA reads the
// group's offsets from the broker itself, so a partial read here is exactly
// when the scaler and the panel would disagree -- which is what the dashboard
// tells the viewer cannot happen.
//
// This test used to assert the opposite -- that the truncated total WAS
// published, and 60 was the expected value. It passed, which is why the defect
// survived: the behaviour was written down as correct rather than noticed.
// Found by an external review pass on 2026-08-30.
func TestPartialReadKeepsPartitionsButNotTheTotal(t *testing.T) {
	resetLagGauges()

	// Phase 1: a complete poll, so there is a known good total to preserve.
	healthy := &fakeBroker{
		partitions: []int{0, 1},
		committed:  map[int]int64{0: 40, 1: 0},
		bounds:     map[int]offsetBounds{0: {first: 0, last: 100}, 1: {first: 0, last: 200}},
	}
	if err := quietPoller(healthy).Refresh(context.Background()); err != nil {
		t.Fatalf("healthy Refresh: %v", err)
	}
	if got := testutil.ToFloat64(ConsumerLagTotal.WithLabelValues(testGroup, testTopic)); got != 260 {
		t.Fatalf("total after a complete poll = %v, want 260", got)
	}
	completeAt := testutil.ToFloat64(LagRefreshedAt)
	if completeAt == 0 {
		t.Fatal("a complete poll did not stamp the refresh timestamp")
	}

	// Phase 2: partition 1 has no leader. Its lag is unknowable this poll.
	degraded := &fakeBroker{
		partitions:   []int{0, 1},
		committed:    map[int]int64{0: 40, 1: 0},
		bounds:       map[int]offsetBounds{0: {first: 0, last: 100}, 1: {first: 0, last: 999}},
		partitionErr: map[int]error{1: errors.New("leader not available")},
	}
	err := quietPoller(degraded).Refresh(context.Background())
	if err == nil {
		t.Fatal("a partial read returned nil; an incomplete poll must be reported, not published as a smaller total")
	}

	// The readable partition is still published: one bad partition should not
	// discard the good ones.
	if got := testutil.ToFloat64(ConsumerLag.WithLabelValues(testGroup, testTopic, "0")); got != 60 {
		t.Errorf("lag on the healthy partition = %v, want 60", got)
	}
	// The total is the previous COMPLETE one, not 60.
	if got := testutil.ToFloat64(ConsumerLagTotal.WithLabelValues(testGroup, testTopic)); got != 260 {
		t.Errorf("total lag = %v, want the last complete total 260 -- a partial sum must not overwrite it", got)
	}
	if got := testutil.ToFloat64(LagRefreshedAt); got != completeAt {
		t.Errorf("refresh timestamp moved on a partial read (%v -> %v); the panel would read the partial total as current", completeAt, got)
	}
	if got := testutil.ToFloat64(LagPartitionsMissing.WithLabelValues(testGroup, testTopic)); got != 1 {
		t.Errorf("partitions missing = %v, want 1", got)
	}
}

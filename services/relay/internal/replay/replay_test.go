package replay

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type fakeClient struct {
	describe func(context.Context, *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error)
	metadata func(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	list     func(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
	commit   func(context.Context, *kafka.OffsetCommitRequest) (*kafka.OffsetCommitResponse, error)
}

func (f fakeClient) DescribeGroups(ctx context.Context, req *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error) {
	return f.describe(ctx, req)
}
func (f fakeClient) Metadata(ctx context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
	return f.metadata(ctx, req)
}
func (f fakeClient) ListOffsets(ctx context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
	return f.list(ctx, req)
}
func (f fakeClient) OffsetCommit(ctx context.Context, req *kafka.OffsetCommitRequest) (*kafka.OffsetCommitResponse, error) {
	return f.commit(ctx, req)
}

func TestResetTimestampUsesMatchedOffsetAndLogEndFallback(t *testing.T) {
	addr := kafka.TCP("broker:9092")
	at := time.Unix(1_700_000_000, 0)
	var committed []kafka.OffsetCommit
	c := fakeClient{
		metadata: func(_ context.Context, req *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
			if req.Addr.String() != addr.String() {
				t.Fatalf("metadata addr = %v", req.Addr)
			}
			return &kafka.MetadataResponse{Topics: []kafka.Topic{{
				Name:       "events",
				Partitions: []kafka.Partition{{ID: 0}, {ID: 1}},
			}}}, nil
		},
		list: func(_ context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error) {
			if got := len(req.Topics["events"]); got != 4 {
				t.Fatalf("offset requests = %d, want timestamp and log end for each partition", got)
			}
			return &kafka.ListOffsetsResponse{Topics: map[string][]kafka.PartitionOffsets{
				"events": {
					{Partition: 1, LastOffset: 42, Offsets: map[int64]time.Time{-1: {}}},
					{Partition: 0, LastOffset: 12, Offsets: map[int64]time.Time{7: at}},
				},
			}}, nil
		},
		commit: func(_ context.Context, req *kafka.OffsetCommitRequest) (*kafka.OffsetCommitResponse, error) {
			if req.GenerationID != -1 || req.MemberID != "" {
				t.Fatalf("administrative commit used generation %d member %q", req.GenerationID, req.MemberID)
			}
			committed = req.Topics["events"]
			return &kafka.OffsetCommitResponse{Topics: map[string][]kafka.OffsetCommitPartition{
				"events": {{Partition: 0}, {Partition: 1}},
			}}, nil
		},
	}

	results, err := Reset(context.Background(), c, addr, "relay-deliver", "events", &at)
	if err != nil {
		t.Fatal(err)
	}
	if len(committed) != 2 || committed[0].Offset != 42 || committed[1].Offset != 7 {
		t.Fatalf("commits = %#v", committed)
	}
	if len(results) != 2 || results[0] != (ResetResult{Partition: 0, Offset: 7}) || results[1] != (ResetResult{Partition: 1, Offset: 42}) {
		t.Fatalf("results = %#v", results)
	}
}

func TestWaitInactivePollsUntilEmpty(t *testing.T) {
	calls := 0
	c := fakeClient{describe: func(context.Context, *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error) {
		calls++
		if calls == 1 {
			return &kafka.DescribeGroupsResponse{Groups: []kafka.DescribeGroupsResponseGroup{{
				GroupID: "relay-deliver", GroupState: "Stable", Members: []kafka.DescribeGroupsResponseMember{{MemberID: "one"}},
			}}}, nil
		}
		return &kafka.DescribeGroupsResponse{Groups: []kafka.DescribeGroupsResponseGroup{{
			GroupID: "relay-deliver", GroupState: "Empty",
		}}}, nil
	}}

	if err := WaitInactive(context.Background(), c, kafka.TCP("broker:9092"), "relay-deliver", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("describe calls = %d, want 2", calls)
	}
}

func TestWaitInactiveTimeoutIncludesLastGroupState(t *testing.T) {
	c := fakeClient{describe: func(context.Context, *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error) {
		return &kafka.DescribeGroupsResponse{Groups: []kafka.DescribeGroupsResponseGroup{{
			GroupID: "relay-deliver", GroupState: "Stable", Members: []kafka.DescribeGroupsResponseMember{{MemberID: "one"}, {MemberID: "two"}},
		}}}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := WaitInactive(ctx, c, kafka.TCP("broker:9092"), "relay-deliver", time.Second)
	if err == nil || !strings.Contains(err.Error(), `last state "Stable" with 2 members`) {
		t.Fatalf("error = %v", err)
	}
}

func TestResetReportsProtocolStage(t *testing.T) {
	c := fakeClient{metadata: func(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error) {
		return nil, errors.New("authentication failed")
	}}
	_, err := Reset(context.Background(), c, staticAddr("broker"), "group", "topic", nil)
	if err == nil || err.Error() != "read topic metadata: authentication failed" {
		t.Fatalf("error = %v", err)
	}
}

type staticAddr string

func (a staticAddr) Network() string { return "tcp" }
func (a staticAddr) String() string  { return string(a) }

var _ net.Addr = staticAddr("")

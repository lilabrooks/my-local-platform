// Package replay moves an inactive relay consumer group's committed offsets.
package replay

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/segmentio/kafka-go"
)

type client interface {
	DescribeGroups(context.Context, *kafka.DescribeGroupsRequest) (*kafka.DescribeGroupsResponse, error)
	Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	ListOffsets(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
	OffsetCommit(context.Context, *kafka.OffsetCommitRequest) (*kafka.OffsetCommitResponse, error)
}

type ResetResult struct {
	Partition int
	Offset    int64
}

// WaitInactive waits until the broker reports no active members. Kafka rejects
// offset commits for a group with members, so callers must stop the consumer
// before entering here.
func WaitInactive(ctx context.Context, c client, addr net.Addr, group string, poll time.Duration) error {
	if poll <= 0 {
		poll = time.Second
	}
	for {
		response, err := c.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
			Addr:     addr,
			GroupIDs: []string{group},
		})
		if err != nil {
			return fmt.Errorf("describe consumer group: %w", err)
		}
		if len(response.Groups) == 0 {
			return nil
		}
		g := response.Groups[0]
		if g.Error != nil {
			if errors.Is(g.Error, kafka.GroupIdNotFound) {
				return nil
			}
			return fmt.Errorf("describe consumer group: %w", g.Error)
		}
		if len(g.Members) == 0 && (g.GroupState == "Empty" || g.GroupState == "Dead" || g.GroupState == "") {
			return nil
		}

		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for consumer group to become inactive: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

// Reset commits the earliest offset, or the first offset at or after at, for
// every current partition of topic. A timestamp after a partition's newest
// record resolves to its log end rather than Kafka's -1 sentinel.
func Reset(ctx context.Context, c client, addr net.Addr, group, topic string, at *time.Time) ([]ResetResult, error) {
	metadata, err := c.Metadata(ctx, &kafka.MetadataRequest{Addr: addr, Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("read topic metadata: %w", err)
	}
	partitions, err := topicPartitions(metadata, topic)
	if err != nil {
		return nil, err
	}

	requests := make([]kafka.OffsetRequest, 0, len(partitions)*2)
	for _, partition := range partitions {
		if at == nil {
			requests = append(requests, kafka.FirstOffsetOf(partition))
		} else {
			requests = append(requests, kafka.TimeOffsetOf(partition, *at), kafka.LastOffsetOf(partition))
		}
	}
	listed, err := c.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   addr,
		Topics: map[string][]kafka.OffsetRequest{topic: requests},
	})
	if err != nil {
		return nil, fmt.Errorf("list replay offsets: %w", err)
	}

	commits, results, err := offsetsFor(topic, listed, at != nil)
	if err != nil {
		return nil, err
	}
	committed, err := c.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		Addr: addr,
		// -1 marks an administrative commit by no current group member. Zero
		// names generation zero and is rejected with UnknownMemberId once the
		// stopped group has a later generation.
		GenerationID: -1,
		GroupID:      group,
		Topics:       map[string][]kafka.OffsetCommit{topic: commits},
	})
	if err != nil {
		return nil, fmt.Errorf("commit replay offsets: %w", err)
	}
	for _, partition := range committed.Topics[topic] {
		if partition.Error != nil {
			return nil, fmt.Errorf("commit replay offset for partition %d: %w", partition.Partition, partition.Error)
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Partition < results[j].Partition })
	return results, nil
}

func topicPartitions(response *kafka.MetadataResponse, topic string) ([]int, error) {
	for _, found := range response.Topics {
		if found.Name != topic {
			continue
		}
		if found.Error != nil {
			return nil, fmt.Errorf("read topic metadata for %q: %w", topic, found.Error)
		}
		if len(found.Partitions) == 0 {
			return nil, fmt.Errorf("topic %q has no partitions", topic)
		}
		partitions := make([]int, len(found.Partitions))
		for i, partition := range found.Partitions {
			if partition.Error != nil {
				return nil, fmt.Errorf("read topic metadata for partition %d: %w", partition.ID, partition.Error)
			}
			partitions[i] = partition.ID
		}
		return partitions, nil
	}
	return nil, fmt.Errorf("topic %q is absent from broker metadata", topic)
}

func offsetsFor(topic string, response *kafka.ListOffsetsResponse, timestamped bool) ([]kafka.OffsetCommit, []ResetResult, error) {
	listed := response.Topics[topic]
	if len(listed) == 0 {
		return nil, nil, fmt.Errorf("broker returned no replay offsets for topic %q", topic)
	}
	commits := make([]kafka.OffsetCommit, 0, len(listed))
	results := make([]ResetResult, 0, len(listed))
	for _, partition := range listed {
		if partition.Error != nil {
			return nil, nil, fmt.Errorf("list replay offset for partition %d: %w", partition.Partition, partition.Error)
		}
		offset := partition.FirstOffset
		if timestamped {
			offset = -1
			for candidate := range partition.Offsets {
				offset = candidate
				break
			}
			if offset < 0 {
				offset = partition.LastOffset
			}
		}
		if offset < 0 {
			return nil, nil, fmt.Errorf("broker returned no usable replay offset for partition %d", partition.Partition)
		}
		commits = append(commits, kafka.OffsetCommit{Partition: partition.Partition, Offset: offset})
		results = append(results, ResetResult{Partition: partition.Partition, Offset: offset})
	}
	return commits, results, nil
}

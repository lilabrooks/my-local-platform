package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

// Lag metrics describe the consumer group as a whole, which is why they are
// published by ingest rather than by the consumers themselves.
//
// A deliver pod only knows the partitions it holds. With KEDA moving the group
// between 1 and 12 members, per-pod lag series appear, vanish and overlap
// across every rebalance, so the sum of them is wrong exactly while the demo is
// being watched. Reading the group's committed offsets from the broker gives
// one stable series per partition however many consumers exist -- which is also
// how KEDA itself decides to scale, so the panel and the scaler are looking at
// the same number rather than two things that ought to agree.
var (
	ConsumerLag = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_consumer_group_lag",
		Help: "Records produced to a partition but not yet committed by the consumer group.",
	}, []string{"group", "topic", "partition"})

	ConsumerLagTotal = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_consumer_group_lag_total",
		Help: "Sum of relay_consumer_group_lag across every partition of a topic.",
	}, []string{"group", "topic"})

	CommittedOffset = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_consumer_group_committed_offset",
		Help: "Last offset committed by the consumer group, or -1 if it has never committed.",
	}, []string{"group", "topic", "partition"})

	HighWatermark = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_partition_high_watermark",
		Help: "Offset one past the last record on a partition.",
	}, []string{"topic", "partition"})

	// Without these two, a poller that has been failing for ten minutes looks
	// identical to a consumer that has caught up: the gauges simply hold their
	// last value. A lag of zero on the demo panel has to be distinguishable
	// from a lag nobody has been able to measure.
	LagRefreshErrors = factory.NewCounter(prometheus.CounterOpts{
		Name: "relay_lag_refresh_errors_total",
		Help: "Failed attempts to read consumer group lag from the broker.",
	})

	LagRefreshedAt = factory.NewGauge(prometheus.GaugeOpts{
		Name: "relay_lag_refreshed_timestamp_seconds",
		Help: "Unix time of the last COMPLETE lag refresh. Stale means the gauges above are stale.",
	})

	// A partial read is the case the two metrics above do not cover. The poll
	// succeeded, so it is not an error; but some partition could not be read,
	// so the total would be a sum over a subset -- a smaller number, stamped
	// fresh, which reads on the panel as a backlog that drained.
	LagPartitionsMissing = factory.NewGaugeVec(prometheus.GaugeOpts{
		Name: "relay_lag_partitions_missing",
		Help: "Partitions whose lag could not be read on the last poll. Non-zero means the total is not published.",
	}, []string{"group", "topic"})
)

// offsetClient is the part of kafka.Client the poller needs, as an interface so
// the arithmetic below is testable without a broker. *kafka.Client satisfies it.
type offsetClient interface {
	Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
	ListOffsets(context.Context, *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
}

// LagPoller refreshes the lag gauges on an interval.
type LagPoller struct {
	client   offsetClient
	group    string
	topic    string
	interval time.Duration
	log      *slog.Logger
}

// NewLagPoller builds a poller against the given brokers.
//
// The interval wants to be short enough that a scrape never returns a value
// older than Prometheus's scrape interval, and long enough that it is not a
// meaningful load on the broker. Three requests every few seconds against a
// twelve-partition topic is nothing.
func NewLagPoller(brokers []string, group, topic string, interval time.Duration, log *slog.Logger) *LagPoller {
	return newLagPoller(&kafka.Client{
		Addr: kafka.TCP(brokers...),
		// Bounded so a broker that accepts the connection and then stalls
		// cannot wedge the poll loop. Well under the interval.
		Timeout: 5 * time.Second,
	}, group, topic, interval, log)
}

func newLagPoller(c offsetClient, group, topic string, interval time.Duration, log *slog.Logger) *LagPoller {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &LagPoller{client: c, group: group, topic: topic, interval: interval, log: log}
}

// Run refreshes until the context is cancelled. It never returns an error: a
// broker that cannot be reached makes the lag gauges stale, which
// relay_lag_refreshed_timestamp_seconds reports, and is not a reason to take
// ingest out of service when it is still accepting events perfectly well.
func (p *LagPoller) Run(ctx context.Context) {
	t := time.NewTicker(p.interval)
	defer t.Stop()

	// Refresh once immediately so the first scrape after startup has data
	// rather than nothing for a whole interval.
	p.refreshLogged(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.refreshLogged(ctx)
		}
	}
}

func (p *LagPoller) refreshLogged(ctx context.Context) {
	if err := p.Refresh(ctx); err != nil {
		if ctx.Err() != nil {
			return // shutting down, not a fault
		}
		LagRefreshErrors.Inc()
		p.log.Warn("lag refresh failed, gauges are now stale",
			"error", err, "group", p.group, "topic", p.topic)
	}
}

// Refresh performs one poll: list the topic's partitions, read the group's
// committed offset on each, read each partition's bounds, and publish the
// difference.
func (p *LagPoller) Refresh(ctx context.Context) error {
	partitions, err := p.partitions(ctx)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		// The topic exists with no partitions, or does not exist yet because
		// bootstrap has not run. Neither is this poller's problem to solve,
		// but reporting lag of zero for a topic nobody can read would be a lie.
		return fmt.Errorf("topic %q reports no partitions", p.topic)
	}

	committed, err := p.committedOffsets(ctx, partitions)
	if err != nil {
		return err
	}
	bounds, err := p.partitionBounds(ctx, partitions)
	if err != nil {
		return err
	}

	var total float64
	var missing int
	for _, partition := range partitions {
		b, ok := bounds[partition]
		if !ok {
			missing++
			continue
		}
		label := strconv.Itoa(partition)
		HighWatermark.WithLabelValues(p.topic, label).Set(float64(b.last))

		offset, ok := committed[partition]
		if !ok {
			missing++
			continue
		}
		CommittedOffset.WithLabelValues(p.group, p.topic, label).Set(float64(offset))

		lag := lagFor(offset, b.first, b.last)
		ConsumerLag.WithLabelValues(p.group, p.topic, label).Set(float64(lag))
		total += float64(lag)
	}

	LagPartitionsMissing.WithLabelValues(p.group, p.topic).Set(float64(missing))

	// The per-partition gauges above are published either way: one unreadable
	// partition should not discard eleven good ones.
	//
	// The TOTAL is different, and publishing a partial one was the defect. A
	// sum over a subset is a smaller number that looks exactly like a backlog
	// draining, and stamping LagRefreshedAt alongside it asserted the smaller
	// number was current. KEDA reads the group's offsets from the broker
	// itself, so during a partial read the scaler and this panel disagree --
	// which is precisely what the dashboard tells the viewer cannot happen.
	//
	// So an incomplete poll leaves the last complete total in place and does
	// not touch the freshness stamp. The panel then shows a total that is
	// visibly ageing, relay_lag_partitions_missing says why, and neither reads
	// as lag going down.
	if missing > 0 {
		return fmt.Errorf("lag for %d of %d partitions on %q could not be read; "+
			"total and refresh timestamp left unchanged", missing, len(partitions), p.topic)
	}

	ConsumerLagTotal.WithLabelValues(p.group, p.topic).Set(total)
	LagRefreshedAt.Set(float64(time.Now().Unix()))
	return nil
}

// lagFor is the whole arithmetic, extracted because the uncommitted case is the
// one that is easy to get wrong and worth a test of its own.
//
// A group that has never committed on a partition reports offset -1. Its lag is
// then not "last minus minus-one" but everything still retained on the
// partition, because relay's readers start at kafka.FirstOffset and will
// deliver all of it. Reporting 0 there would show a drained topic at exactly
// the moment a fresh consumer group faces its largest backlog -- and that is
// the moment the demo starts from.
func lagFor(committed, first, last int64) int64 {
	if committed < 0 {
		committed = first
	}
	if lag := last - committed; lag > 0 {
		return lag
	}
	// A committed offset ahead of the high watermark is not arithmetic to
	// publish as negative lag; it means the partition was truncated or the
	// topic recreated under the group.
	return 0
}

func (p *LagPoller) partitions(ctx context.Context) ([]int, error) {
	res, err := p.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{p.topic}})
	if err != nil {
		return nil, fmt.Errorf("topic metadata: %w", err)
	}
	for _, topic := range res.Topics {
		if topic.Name != p.topic {
			continue
		}
		if topic.Error != nil {
			return nil, fmt.Errorf("topic %q metadata: %w", p.topic, topic.Error)
		}
		ids := make([]int, 0, len(topic.Partitions))
		for _, part := range topic.Partitions {
			ids = append(ids, part.ID)
		}
		return ids, nil
	}
	return nil, fmt.Errorf("topic %q not present in metadata", p.topic)
}

func (p *LagPoller) committedOffsets(ctx context.Context, partitions []int) (map[int]int64, error) {
	res, err := p.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: p.group,
		Topics:  map[string][]int{p.topic: partitions},
	})
	if err != nil {
		return nil, fmt.Errorf("offset fetch for group %q: %w", p.group, err)
	}
	if res.Error != nil {
		return nil, fmt.Errorf("offset fetch for group %q: %w", p.group, res.Error)
	}

	out := make(map[int]int64, len(partitions))
	var partitionErrs []error
	for _, part := range res.Topics[p.topic] {
		if part.Error != nil {
			partitionErrs = append(partitionErrs,
				fmt.Errorf("partition %d: %w", part.Partition, part.Error))
			continue
		}
		out[part.Partition] = part.CommittedOffset
	}
	// One bad partition should not discard eleven good ones, but it must not
	// pass silently either -- a partition missing from the map is a gap in the
	// total that would otherwise read as lag going down.
	if len(out) == 0 && len(partitionErrs) > 0 {
		return nil, errors.Join(partitionErrs...)
	}
	if len(partitionErrs) > 0 {
		p.log.Warn("some partitions have no committed offset",
			"error", errors.Join(partitionErrs...), "group", p.group, "topic", p.topic)
	}
	return out, nil
}

type offsetBounds struct{ first, last int64 }

func (p *LagPoller) partitionBounds(ctx context.Context, partitions []int) (map[int]offsetBounds, error) {
	// Both ends of every partition in one request: kafka-go merges the two
	// OffsetRequests per partition into a single PartitionOffsets entry.
	requests := make([]kafka.OffsetRequest, 0, len(partitions)*2)
	for _, partition := range partitions {
		requests = append(requests, kafka.FirstOffsetOf(partition), kafka.LastOffsetOf(partition))
	}

	res, err := p.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{p.topic: requests},
	})
	if err != nil {
		return nil, fmt.Errorf("list offsets for topic %q: %w", p.topic, err)
	}

	out := make(map[int]offsetBounds, len(partitions))
	var partitionErrs []error
	for _, part := range res.Topics[p.topic] {
		if part.Error != nil {
			partitionErrs = append(partitionErrs,
				fmt.Errorf("partition %d: %w", part.Partition, part.Error))
			continue
		}
		out[part.Partition] = offsetBounds{first: part.FirstOffset, last: part.LastOffset}
	}
	if len(out) == 0 && len(partitionErrs) > 0 {
		return nil, errors.Join(partitionErrs...)
	}
	if len(partitionErrs) > 0 {
		p.log.Warn("some partitions have no readable offsets",
			"error", errors.Join(partitionErrs...), "topic", p.topic)
	}
	return out, nil
}

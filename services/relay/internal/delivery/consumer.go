package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/history"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
	"github.com/lilabrooks/my-local-platform/relay/internal/telemetry"
)

// Reader is the part of kafka.Reader the consumer needs. An interface so the
// loop is testable without a broker.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer writes dead-letter records.
type Producer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// SubscriptionSource answers where a tenant's events go.
type SubscriptionSource interface {
	ForTenant(ctx context.Context, tenantID string) ([]subscriptions.Subscription, error)
}

// DeadLetter is what lands on the dead-letter topic.
//
// It carries the original record so the event can be replayed, and enough about
// the failure to act on it. It deliberately does NOT carry the signing secret;
// only the subscription's identity and URL.
type DeadLetter struct {
	Record         event.Record `json:"record"`
	SubscriptionID int64        `json:"subscription_id"`
	URL            string       `json:"url"`
	Attempts       int          `json:"attempts"`
	LastStatus     int          `json:"last_status,omitempty"`
	Reason         string       `json:"reason"`
	FailedAt       time.Time    `json:"failed_at"`
}

// Consumer reads the delivery topic and fans each record out to its tenant's
// subscribers.
type Consumer struct {
	reader        Reader
	dlq           Producer
	subs          SubscriptionSource
	deliverer     *Deliverer
	recordTimeout time.Duration
	log           *slog.Logger

	ready        atomic.Bool
	handled      atomic.Int64
	delivered    atomic.Int64
	deadLettered atomic.Int64
}

// NewConsumer wires the pieces together. recordTimeout bounds the complete
// handle-to-commit work for one fetched record, including a graceful-shutdown
// drain.
func NewConsumer(
	r Reader,
	dlq Producer,
	subs SubscriptionSource,
	d *Deliverer,
	recordTimeout time.Duration,
	log *slog.Logger,
) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{
		reader: r, dlq: dlq, subs: subs, deliverer: d,
		recordTimeout: recordTimeout, log: log,
	}
}

// MarkReady flips readiness.
func (c *Consumer) MarkReady(ready bool) { c.ready.Store(ready) }

// Ready reports readiness.
func (c *Consumer) Ready() bool { return c.ready.Load() }

// Stats is what the readiness endpoint reports.
func (c *Consumer) Stats() map[string]int64 {
	return map[string]int64{
		"handled":       c.handled.Load(),
		"delivered":     c.delivered.Load(),
		"dead_lettered": c.deadLettered.Load(),
	}
}

// Run consumes until the context is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	if c.recordTimeout <= 0 {
		return fmt.Errorf("record timeout %s must be positive", c.recordTimeout)
	}

	c.MarkReady(true)
	defer c.MarkReady(false)
	// Stop advertising readiness as soon as shutdown begins. A record already in
	// hand still drains below; new work should go to members that are staying.
	stopReadiness := context.AfterFunc(ctx, func() { c.MarkReady(false) })
	defer stopReadiness()

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.log.Info("consumer stopping")
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}

		if err := c.processRecord(ctx, msg); err != nil {
			if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				// The record used its complete drain allowance. Leave the offset
				// uncommitted and exit before the orchestrator reaches SIGKILL.
				c.log.Warn("shutdown drain expired, record will be redelivered",
					"partition", msg.Partition, "offset", msg.Offset,
					"record_timeout", c.recordTimeout, "error", err)
				return nil
			}
			// Every failure leaves the offset uncommitted, so the record comes
			// back rather than being silently skipped.
			return err
		}

		if ctx.Err() != nil {
			c.log.Info("consumer drained current record and is stopping",
				"partition", msg.Partition, "offset", msg.Offset)
			return nil
		}
	}
}

// processRecord gives work already fetched its own deadline. The parent
// context stops the next FetchMessage and marks readiness false, while
// context.WithoutCancel lets this record finish after SIGTERM. The timeout keeps
// that drain inside the pod's termination grace period.
func (c *Consumer) processRecord(parent context.Context, msg kafka.Message) error {
	base := context.WithoutCancel(parent)
	base = otel.GetTextMapPropagator().Extract(base, telemetry.NewKafkaHeaderCarrier(&msg.Headers))
	ctx, cancel := context.WithTimeout(base, c.recordTimeout)
	defer cancel()
	ctx, span := otel.Tracer("relay/delivery").Start(ctx, "relay.consume",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		),
	)
	defer span.End()

	start := time.Now()
	if err := c.handle(ctx, msg); err != nil {
		telemetry.RecordError(span, err, "handle record")
		return fmt.Errorf("handle partition %d offset %d: %w", msg.Partition, msg.Offset, err)
	}

	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		telemetry.RecordError(span, err, "commit offset")
		return fmt.Errorf("commit partition %d offset %d: %w", msg.Partition, msg.Offset, err)
	}
	// The committed offset is an event, not a status. handle can park an
	// undecodable record on the dead-letter queue and still return nil, and it
	// marks that span Error; setting Ok here would erase it, because the
	// OpenTelemetry SDK lets Ok override Error and never the reverse
	// (sdk/trace: SetStatus returns early when status.Code > code). Unset is
	// the right status for "finished, nothing to report".
	span.AddEvent("kafka.offset.committed")
	c.handled.Add(1)
	// Counted after the commit, so the metric means "finished with", the same
	// thing the committed offset means. Counting at fetch time would include
	// records the consumer was interrupted part-way through.
	metrics.RecordsConsumed.WithLabelValues(strconv.Itoa(msg.Partition)).Inc()
	metrics.RecordDuration.Observe(time.Since(start).Seconds())
	return nil
}

// handle processes one record. Returning nil means the offset may be committed.
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	span := trace.SpanFromContext(ctx)
	var rec event.Record
	if err := json.Unmarshal(msg.Value, &rec); err != nil {
		// A record that will never parse cannot be fixed by redelivering it.
		// Park it and move on, or it blocks the partition forever.
		c.log.Error("undecodable record, dead-lettering",
			"error", err, "partition", msg.Partition, "offset", msg.Offset)
		metrics.DeadLetters.WithLabelValues("undecodable").Inc()
		if err := c.deadLetter(ctx, DeadLetter{
			Reason:   fmt.Sprintf("undecodable record at partition %d offset %d: %v", msg.Partition, msg.Offset, err),
			FailedAt: time.Now().UTC(),
		}); err != nil {
			return err
		}
		// This path parks an event and then returns nil so the offset advances.
		// Without an event and a status here the trace is indistinguishable
		// from a clean delivery -- and the record has no id to search by, so
		// the span is the only place the loss is visible at all.
		span.AddEvent("relay.dead_lettered", trace.WithAttributes(
			attribute.String("relay.dead_letter.reason", "undecodable"),
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		))
		span.SetStatus(codes.Error, "undecodable record dead-lettered")
		return nil
	}
	span.SetAttributes(
		attribute.String("relay.event.id", rec.ID),
		attribute.String("relay.tenant.id", rec.TenantID),
		attribute.String("relay.event.type", rec.Type),
	)

	subs, err := c.subs.ForTenant(ctx, rec.TenantID)
	if err != nil {
		// The tenant is already an attribute; the event says which lookup, and
		// the classification says what kind of failure. processRecord sets the
		// span's error status from the error this returns.
		span.AddEvent("relay.subscription_lookup_failed", trace.WithAttributes(
			attribute.String("error.type", telemetry.ErrorType(err)),
		))
		// A database blip is not the event's fault. Leave the offset alone so
		// the record is redelivered rather than lost.
		return fmt.Errorf("look up subscriptions for %q: %w", rec.TenantID, err)
	}
	if len(subs) == 0 {
		c.log.Info("no subscribers", "event_id", rec.ID, "tenant", rec.TenantID)
		return nil
	}

	// Concurrent per subscriber, each with its own retry budget. Sequential
	// would make a healthy subscriber wait out a failing one's entire
	// schedule, which is exactly what "one failing subscriber does not stop
	// delivery to a healthy one" forbids.
	outcomes := make([]Outcome, len(subs))
	errs := make([]error, len(subs))
	var wg sync.WaitGroup

	for i, sub := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outcomes[i], errs[i] = c.deliverer.Deliver(ctx, sub, rec)
		}()
	}
	wg.Wait()

	// Return what actually failed, rather than rebuilding an error from the
	// context.
	//
	// This used to record "something failed" in a bool and then return
	// context.Cause(ctx). The two agreed only while Deliver returned nothing
	// but ctx.Err() -- an invariant held in a different function and enforced
	// nowhere. For any other error the cause was nil, handle returned nil, and
	// the caller committed the offset as though every subscriber had
	// succeeded. Silent data loss, and Deliver is one line from producing it:
	// it returns d.sleep's error verbatim.
	//
	// A missing event row cannot repair itself on Kafka redelivery. Treat it as
	// terminal after every other subscriber error has cleared, or one orphaned
	// record crash-loops the consumer and blocks its partition forever.
	historyMissing := make([]bool, len(errs))
	retryErrors := make([]error, 0, len(errs))
	for i, err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, history.ErrEventNotFound) {
			historyMissing[i] = true
			continue
		}
		retryErrors = append(retryErrors, err)
	}
	// Joined rather than first-wins because each subscriber has its own budget
	// and losing the others' failures is how this went wrong the first time.
	// errors.Join is nil when every element is nil, and errors.Is still reaches
	// a context.Canceled inside it, so Run's shutdown path is unchanged.
	if err := errors.Join(retryErrors...); err != nil {
		return err
	}
	for i, missing := range historyMissing {
		if !missing {
			continue
		}
		out := outcomes[i]
		reason := fmt.Sprintf("event history missing after attempt %d: %v", out.Attempts, errs[i])
		c.log.Error("event history missing, dead-lettering",
			"event_id", rec.ID, "tenant", rec.TenantID,
			"url", out.Subscription.URL, "attempts", out.Attempts,
			"status", out.LastStatus, "delivered", out.Delivered)
		if err := c.deadLetter(ctx, DeadLetter{
			Record:         rec,
			SubscriptionID: out.Subscription.ID,
			URL:            out.Subscription.URL,
			Attempts:       out.Attempts,
			LastStatus:     out.LastStatus,
			Reason:         reason,
			FailedAt:       time.Now().UTC(),
		}); err != nil {
			return fmt.Errorf("dead-letter missing history for %s: %w", rec.ID, err)
		}
		span.AddEvent("relay.dead_lettered", trace.WithAttributes(
			attribute.Int64("relay.subscription.id", out.Subscription.ID),
			attribute.String("relay.dead_letter.reason", "history_missing"),
		))
		if out.Delivered {
			c.delivered.Add(1)
			metrics.Deliveries.WithLabelValues("delivered").Inc()
		} else {
			c.deadLettered.Add(1)
			metrics.Deliveries.WithLabelValues("dead_lettered").Inc()
		}
		metrics.DeadLetters.WithLabelValues("history_missing").Inc()
	}

	// Only once every subscriber has reached a terminal state is the record
	// finished. Dead-letter the failures before the caller commits.
	for i, out := range outcomes {
		if historyMissing[i] {
			continue
		}
		if out.Delivered {
			c.delivered.Add(1)
			metrics.Deliveries.WithLabelValues("delivered").Inc()
			c.log.Info("delivered", "event_id", rec.ID, "tenant", rec.TenantID,
				"url", out.Subscription.URL, "attempts", out.Attempts, "status", out.LastStatus)
			continue
		}
		c.log.Warn("dead-lettering", "event_id", rec.ID, "tenant", rec.TenantID,
			"url", out.Subscription.URL, "attempts", out.Attempts, "reason", out.Reason)
		if err := c.deadLetter(ctx, DeadLetter{
			Record:         rec,
			SubscriptionID: out.Subscription.ID,
			URL:            out.Subscription.URL,
			Attempts:       out.Attempts,
			LastStatus:     out.LastStatus,
			Reason:         out.Reason,
			FailedAt:       time.Now().UTC(),
		}); err != nil {
			// The offset must not advance past a record whose failure was not
			// recorded. A duplicate delivery on redelivery is recoverable; a
			// silently dropped event is not.
			return fmt.Errorf("dead-letter %s: %w", rec.ID, err)
		}
		span.AddEvent("relay.dead_lettered", trace.WithAttributes(
			attribute.Int64("relay.subscription.id", out.Subscription.ID),
			attribute.String("relay.dead_letter.reason", "delivery_exhausted"),
		))
		c.deadLettered.Add(1)
		metrics.Deliveries.WithLabelValues("dead_lettered").Inc()
		metrics.DeadLetters.WithLabelValues("exhausted").Inc()
	}
	return nil
}

func (c *Consumer) deadLetter(ctx context.Context, dl DeadLetter) error {
	value, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	// Keyed by tenant like the source topic, so a tenant's failures stay
	// together and in order.
	return c.dlq.WriteMessages(ctx, kafka.Message{
		Key:   []byte(dl.Record.TenantID),
		Value: value,
	})
}

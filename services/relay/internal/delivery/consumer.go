package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
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
	reader    Reader
	dlq       Producer
	subs      SubscriptionSource
	deliverer *Deliverer
	log       *slog.Logger

	ready        atomic.Bool
	handled      atomic.Int64
	delivered    atomic.Int64
	deadLettered atomic.Int64
}

// NewConsumer wires the pieces together.
func NewConsumer(r Reader, dlq Producer, subs SubscriptionSource, d *Deliverer, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{reader: r, dlq: dlq, subs: subs, deliverer: d, log: log}
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
	c.MarkReady(true)
	defer c.MarkReady(false)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.log.Info("consumer stopping")
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}

		if err := c.handle(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Shutting down mid-record. Do not commit: the record is
				// redelivered, which is at-least-once working as designed.
				c.log.Info("interrupted before commit, record will be redelivered",
					"partition", msg.Partition, "offset", msg.Offset)
				return nil
			}
			// Any other failure also leaves the offset uncommitted, so the
			// record comes back rather than being silently skipped.
			return fmt.Errorf("handle partition %d offset %d: %w", msg.Partition, msg.Offset, err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("commit partition %d offset %d: %w", msg.Partition, msg.Offset, err)
		}
		c.handled.Add(1)
	}
}

// handle processes one record. Returning nil means the offset may be committed.
func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var rec event.Record
	if err := json.Unmarshal(msg.Value, &rec); err != nil {
		// A record that will never parse cannot be fixed by redelivering it.
		// Park it and move on, or it blocks the partition forever.
		c.log.Error("undecodable record, dead-lettering",
			"error", err, "partition", msg.Partition, "offset", msg.Offset)
		return c.deadLetter(ctx, DeadLetter{
			Reason:   fmt.Sprintf("undecodable record at partition %d offset %d: %v", msg.Partition, msg.Offset, err),
			FailedAt: time.Now().UTC(),
		})
	}

	subs, err := c.subs.ForTenant(ctx, rec.TenantID)
	if err != nil {
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
	var wg sync.WaitGroup
	var interrupted atomic.Bool

	for i, sub := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := c.deliverer.Deliver(ctx, sub, rec)
			if err != nil {
				interrupted.Store(true)
			}
			outcomes[i] = out
		}()
	}
	wg.Wait()

	if interrupted.Load() {
		return context.Cause(ctx)
	}

	// Only once every subscriber has reached a terminal state is the record
	// finished. Dead-letter the failures before the caller commits.
	for _, out := range outcomes {
		if out.Delivered {
			c.delivered.Add(1)
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
		c.deadLettered.Add(1)
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

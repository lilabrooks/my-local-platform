package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
	"github.com/lilabrooks/my-local-platform/relay/internal/subscriptions"
)

// Outcome is how one subscriber's delivery ended.
type Outcome struct {
	Subscription subscriptions.Subscription
	// Attempts is how many HTTP requests were made, including the first.
	Attempts int
	// Delivered is true when a 2xx was seen. False means the retry budget was
	// spent and the event belongs in the dead-letter queue.
	Delivered bool
	// LastStatus is the final HTTP status, or 0 if no response was received.
	LastStatus int
	// Reason explains a failure. Empty on success. It ends up in the DLQ
	// record, so it is written for whoever reads that later.
	Reason string
}

// Deliverer sends one event to one subscriber, retrying on the configured
// schedule until it succeeds or the budget is spent.
type Deliverer struct {
	client   *http.Client
	schedule config.RetrySchedule
	// timeout bounds a single attempt. The schedule and this together are what
	// ValidateStallBudget checks against the stall budget.
	timeout time.Duration
	// sleep is time.Sleep in production and instant in tests, so a test of the
	// retry logic does not actually wait out the schedule.
	sleep func(context.Context, time.Duration) error
}

// NewDeliverer builds a Deliverer. The schedule must already have passed
// ValidateStallBudget.
func NewDeliverer(schedule config.RetrySchedule, timeout time.Duration) *Deliverer {
	return &Deliverer{
		client: &http.Client{
			// No shared timeout: each attempt gets its own context, so a
			// per-attempt bound cannot be silently overridden here.
			//
			// Redirects are never followed. A subscriber URL is operator
			// supplied, and following a 3xx would let one redirect relay's
			// signed payload to an address nobody configured. A redirect is
			// also not an acknowledgement that the event was processed, so it
			// counts as a failed attempt.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		schedule: schedule,
		timeout:  timeout,
		sleep:    sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Deliver POSTs the record's payload to one subscription, retrying on the
// schedule. It returns an Outcome rather than an error for a failed delivery:
// exhausting the budget is a normal, expected result that gets dead-lettered,
// not an operational fault.
//
// It only returns an error when the context is cancelled, which means the
// consumer is SHUTTING DOWN and the offset must not be committed.
//
// Not a rebalance. This comment used to claim both, and the rebalance half was
// false: the context here descends from signal.NotifyContext in runDeliver and
// nothing links it to consumer-group membership, because kafka-go performs
// joins, heartbeats and generation changes on background goroutines that never
// touch a caller's context. An old partition owner therefore keeps delivering
// after the partition has moved.
//
// The commit-only-when-finished invariant does not depend on that cancellation,
// so it still holds -- but it was documented as if it did, which is why the
// claim is corrected here rather than quietly dropped. See issue #54 and the
// backlog entry for what is being lived with and what would change it.
func (d *Deliverer) Deliver(ctx context.Context, sub subscriptions.Subscription, rec event.Record) (Outcome, error) {
	body, err := json.Marshal(rec.Payload())
	if err != nil {
		// A record that cannot be marshalled will never deliver, so retrying
		// is pointless. Dead-letter it immediately.
		return Outcome{
			Subscription: sub,
			Attempts:     0,
			Reason:       fmt.Sprintf("encode payload: %v", err),
		}, nil
	}

	out := Outcome{Subscription: sub}

	for attempt := 1; attempt <= d.schedule.MaxAttempts(); attempt++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}

		out.Attempts = attempt
		status, reqErr := d.attempt(ctx, sub, rec, body)
		out.LastStatus = status

		switch {
		case reqErr != nil && ctx.Err() != nil:
			// Shutting down, not a subscriber failure. (Not rebalancing: see
			// the note on Deliver above -- nothing cancels this context on a
			// generation change.)
			return out, ctx.Err()
		case reqErr != nil:
			out.Reason = fmt.Sprintf("attempt %d: %v", attempt, reqErr)
		case status >= 200 && status < 300:
			out.Delivered = true
			out.Reason = ""
			return out, nil
		default:
			// Anything not 2xx is a failure, including 3xx: a redirect is not
			// an acknowledgement that the event was processed.
			out.Reason = fmt.Sprintf("attempt %d: HTTP %d", attempt, status)
		}

		delay, ok := d.schedule.DelayFor(attempt)
		if !ok {
			break // budget spent
		}
		if err := d.sleep(ctx, delay); err != nil {
			return out, err
		}
	}

	out.Reason = fmt.Sprintf("gave up after %d attempts: %s", out.Attempts, out.Reason)
	return out, nil
}

// attempt makes one HTTP request and reports its status.
func (d *Deliverer) attempt(ctx context.Context, sub subscriptions.Subscription, rec event.Record, body []byte) (int, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}

	// The timestamp is regenerated per attempt so a retry hours later is not
	// rejected as a replay, but the id stays fixed across every attempt --
	// that is what lets a subscriber dedupe.
	now := time.Now()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "mlp-relay")
	req.Header.Set(HeaderID, rec.ID)
	req.Header.Set(HeaderTimestamp, Timestamp(now))
	req.Header.Set(HeaderSignature, Sign([]byte(sub.Secret), rec.ID, now, body))

	// Timed around the round trip including the drain below, because a
	// subscriber that answers headers promptly and then dribbles the body is
	// occupying this consumer for the whole of it. That occupancy is what
	// becomes lag, so it is what the histogram has to measure.
	start := time.Now()
	resp, err := d.client.Do(req)
	if err != nil {
		observeAttempt(0, time.Since(start))
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	// Drain a bounded amount so the connection can be reused. A subscriber
	// that returns a large body on error should not cost us a new connection
	// per retry.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))

	observeAttempt(resp.StatusCode, time.Since(start))
	return resp.StatusCode, nil
}

// observeAttempt records one HTTP attempt under its status class. A status of
// 0 means no response arrived, which metrics.StatusClass reports as "error"
// rather than folding it into 5xx -- a refused connection and a subscriber
// replying 500 need different fixes.
func observeAttempt(status int, took time.Duration) {
	class := metrics.StatusClass(status)
	metrics.DeliveryAttempts.WithLabelValues(class).Inc()
	metrics.AttemptDuration.WithLabelValues(class).Observe(took.Seconds())
}

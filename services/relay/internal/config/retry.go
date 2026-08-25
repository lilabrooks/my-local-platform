// Package config resolves relay's settings from the environment and refuses to
// start on a combination that cannot work.
package config

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// RetrySchedule is the ordered list of waits between delivery attempts.
//
// Its length is the whole budget: there is deliberately no separate
// max-attempts setting, because two knobs that can disagree are a defect
// waiting to be filed. N delays means up to N+1 delivery attempts -- one
// immediate, then one after each delay -- and Total is the longest an event can
// sit before it is dead-lettered.
type RetrySchedule struct {
	// Name is the preset this came from, or "custom" for an explicit list.
	Name string
	// Delays are the waits before each retry, in order.
	Delays []time.Duration
	// Jitter spreads each delay so a batch of deliveries failing together does
	// not retry in lockstep. Off makes the schedule deterministic for tests.
	Jitter bool
}

// Presets. `standard` is Svix's published schedule, which Standard Webhooks
// recommends: https://docs.svix.com/retries. Eight attempts -- one immediate,
// then these seven waits -- spanning 27h35m5s. Each period starts after the
// preceding attempt fails, so these are gaps and not elapsed times; the
// specification's own worked example is that three failures then a success
// delivers 35m5s after the first attempt, which is 5s+5m+30m.
//
// `demo` reaches the dead-letter queue in 15 seconds, because a demo nobody
// watches for a day is not a demo.
//
// Note that `standard` will not pass ValidateLiveness against any sane
// rebalance timeout. That is intended: enabling it is what forces the
// long-retry parking mechanism, which is M3 work. See
// docs/adr/0006-kafka-over-sqs-for-delivery.md.
var presets = map[string][]time.Duration{
	"standard": {
		5 * time.Second,
		5 * time.Minute,
		30 * time.Minute,
		2 * time.Hour,
		5 * time.Hour,
		10 * time.Hour,
		10 * time.Hour,
	},
	"demo": {
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
	},
}

// ErrEmptySchedule is returned for a schedule with no delays in it. A zero
// retry budget is more likely a typo than a decision; spell it "0s" to mean
// "one attempt, retry immediately once".
var ErrEmptySchedule = errors.New("retry schedule is empty")

// ParseRetrySchedule reads a preset name ("standard", "demo") or an explicit
// comma-separated duration list such as "1s,2s,4s,8s".
//
// One setting accepts both so a preset and an override can never be configured
// to contradict each other.
func ParseRetrySchedule(spec string, jitter bool) (RetrySchedule, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return RetrySchedule{}, ErrEmptySchedule
	}

	if delays, ok := presets[spec]; ok {
		// Copy: callers must not be able to mutate the shared preset.
		out := make([]time.Duration, len(delays))
		copy(out, delays)
		return RetrySchedule{Name: spec, Delays: out, Jitter: jitter}, nil
	}

	// Not a preset. If it looks like a bare word rather than a duration list,
	// say so plainly -- "time: invalid duration" for the input "standrad" sends
	// the reader looking in the wrong place.
	if !strings.ContainsAny(spec, ",0123456789") {
		return RetrySchedule{}, fmt.Errorf("unknown retry preset %q, want one of %s or a duration list like \"1s,2s,4s\"",
			spec, strings.Join(presetNames(), ", "))
	}

	fields := strings.Split(spec, ",")
	delays := make([]time.Duration, 0, len(fields))
	for i, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			return RetrySchedule{}, fmt.Errorf("retry delay %d is empty in %q", i+1, spec)
		}
		d, err := time.ParseDuration(f)
		if err != nil {
			return RetrySchedule{}, fmt.Errorf("retry delay %d (%q): %w", i+1, f, err)
		}
		if d < 0 {
			return RetrySchedule{}, fmt.Errorf("retry delay %d (%q) is negative", i+1, f)
		}
		delays = append(delays, d)
	}
	if len(delays) == 0 {
		return RetrySchedule{}, ErrEmptySchedule
	}

	return RetrySchedule{Name: "custom", Delays: delays, Jitter: jitter}, nil
}

// Retries is how many retries follow the first attempt.
func (s RetrySchedule) Retries() int { return len(s.Delays) }

// MaxAttempts is the total number of delivery attempts: the immediate one plus
// one per delay.
func (s RetrySchedule) MaxAttempts() int { return len(s.Delays) + 1 }

// Total is the sum of every delay -- the longest an event can sit before being
// dead-lettered, ignoring the time each attempt itself takes.
func (s RetrySchedule) Total() time.Duration {
	var total time.Duration
	for _, d := range s.Delays {
		total += d
	}
	return total
}

// DelayFor returns the wait before the given retry, counting from 1. It returns
// false once the budget is spent, which is the signal to dead-letter.
func (s RetrySchedule) DelayFor(retry int) (time.Duration, bool) {
	if retry < 1 || retry > len(s.Delays) {
		return 0, false
	}
	// math/rand/v2 rather than crypto/rand on purpose: this spreads retries so
	// a batch of deliveries failing together does not hammer a recovering
	// subscriber in lockstep. Unpredictability is not a requirement.
	return s.jittered(s.Delays[retry-1], rand.Float64()), true
}

// jittered applies equal jitter: half the delay is fixed, half is spread over
// [0, d/2). Full jitter would allow a near-zero wait, which turns the first
// entry of a schedule into an immediate retry against a subscriber that is
// still down.
func (s RetrySchedule) jittered(d time.Duration, frac float64) time.Duration {
	if !s.Jitter || d == 0 {
		return d
	}
	half := d / 2
	return half + time.Duration(frac*float64(half))
}

// ValidateLiveness rejects a schedule that outlives the consumer's place in its
// group.
//
// relay retries in process, holding the record's partition while it waits. In
// segmentio/kafka-go a background goroutine keeps heartbeating throughout, so a
// sleeping consumer is never dropped for being slow -- there is no
// max.poll.interval.ms here, unlike the Java client. Two things go wrong
// instead:
//
//   - Nothing else on that partition moves for the whole wait. One subscriber
//     that is down stalls every tenant hashing to the same partition.
//   - A rebalance during the wait is not survivable. The coordinator gives
//     members RebalanceTimeout to rejoin, and a consumer asleep in a retry
//     misses it, so its partitions are reassigned and the record is redelivered
//     elsewhere. KEDA scaling makes rebalances routine rather than rare, which
//     is exactly the M2 demo.
//
// So the bound is the rebalance timeout, not a poll interval. Long schedules
// need the record parked and the offset committed -- tiered retry topics, or a
// due-at row with a scheduler -- which is M3 work.
func (s RetrySchedule) ValidateLiveness(rebalanceTimeout, attemptTimeout time.Duration) error {
	if len(s.Delays) == 0 {
		return ErrEmptySchedule
	}
	if attemptTimeout <= 0 {
		return fmt.Errorf("delivery attempt timeout %s must be positive", attemptTimeout)
	}
	// Worst case for one record is every attempt timing out with every delay
	// waited in full. Summing only the delays understates it by the time the
	// attempts themselves take, which for a short schedule is most of it.
	worst := s.WorstCase(attemptTimeout)
	if worst >= rebalanceTimeout {
		return fmt.Errorf(
			"retry schedule %q needs up to %s per record (%s of delays plus %d attempts at %s), "+
				"which is not under the %s rebalance timeout: a consumer busy that long cannot "+
				"rejoin its group, so the delivery is reassigned and redelivered. Shorten the "+
				"schedule, shorten RELAY_DELIVERY_TIMEOUT, or implement long-retry parking "+
				"(see docs/adr/0006-kafka-over-sqs-for-delivery.md)",
			s.Name, worst, s.Total(), s.MaxAttempts(), attemptTimeout, rebalanceTimeout)
	}
	return nil
}

// WorstCase is the longest one record can occupy its partition: every delay
// waited in full, and every attempt running to its timeout.
func (s RetrySchedule) WorstCase(attemptTimeout time.Duration) time.Duration {
	return s.Total() + time.Duration(s.MaxAttempts())*attemptTimeout
}

// String is what gets logged at startup, so the longest an event can sit before
// dead-lettering is something an operator reads rather than computes.
func (s RetrySchedule) String() string {
	parts := make([]string, len(s.Delays))
	for i, d := range s.Delays {
		parts[i] = d.String()
	}
	return fmt.Sprintf("%s: %d attempts over at most %s (delays %s, jitter %v)",
		s.Name, s.MaxAttempts(), s.Total(), strings.Join(parts, ","), s.Jitter)
}

func presetNames() []string {
	names := make([]string, 0, len(presets))
	for n := range presets {
		names = append(names, n)
	}
	// Stable order for a stable error message.
	for i := range names {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseRetrySchedulePresets(t *testing.T) {
	t.Parallel()

	demo, err := ParseRetrySchedule("demo", false)
	if err != nil {
		t.Fatalf("demo preset: %v", err)
	}
	// The demo preset exists so the DLQ is reachable while someone is watching.
	// If this total creeps up, the M2 demo stops working.
	if got, want := demo.Total(), 15*time.Second; got != want {
		t.Errorf("demo total = %s, want %s", got, want)
	}
	if got, want := demo.MaxAttempts(), 5; got != want {
		t.Errorf("demo attempts = %d, want %d", got, want)
	}

	std, err := ParseRetrySchedule("standard", false)
	if err != nil {
		t.Fatalf("standard preset: %v", err)
	}
	// Svix's published schedule: 8 attempts over 27h35m5s. Asserted exactly,
	// because an earlier version of this preset was copied from a secondhand
	// summary that ended 16h instead of 10h and claimed a ~24h budget. This
	// test is what caught it.
	if got, want := std.Total(), 27*time.Hour+35*time.Minute+5*time.Second; got != want {
		t.Errorf("standard total = %s, want %s", got, want)
	}
	if got, want := std.MaxAttempts(), 8; got != want {
		t.Errorf("standard attempts = %d, want %d", got, want)
	}
	// The specification's own worked example: three failures then a success
	// delivers 35m5s after the first attempt.
	var afterThreeFailures time.Duration
	for retry := 1; retry <= 3; retry++ {
		d, ok := std.DelayFor(retry)
		if !ok {
			t.Fatalf("DelayFor(%d) not ok", retry)
		}
		afterThreeFailures += d
	}
	if got, want := afterThreeFailures, 35*time.Minute+5*time.Second; got != want {
		t.Errorf("three failures then success = %s, want %s (docs.svix.com/retries)", got, want)
	}
}

// Mutating a parsed schedule must not corrupt the preset for the next caller.
func TestParseRetrySchedulePresetNotAliased(t *testing.T) {
	t.Parallel()

	first, err := ParseRetrySchedule("demo", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	first.Delays[0] = 999 * time.Hour

	second, err := ParseRetrySchedule("demo", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if second.Delays[0] != time.Second {
		t.Errorf("preset was mutated through a returned slice: got %s, want 1s", second.Delays[0])
	}
}

func TestParseRetryScheduleExplicit(t *testing.T) {
	t.Parallel()

	s, err := ParseRetrySchedule("1s, 2s ,4s", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got, want := s.Name, "custom"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	if got, want := s.Total(), 7*time.Second; got != want {
		t.Errorf("total = %s, want %s", got, want)
	}
	if got, want := s.Retries(), 3; got != want {
		t.Errorf("retries = %d, want %d", got, want)
	}
}

func TestParseRetryScheduleRejects(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		spec string
		want string // substring the message must carry
	}{
		"empty":           {"", "empty"},
		"whitespace only": {"   ", "empty"},
		"unknown preset":  {"standrad", "unknown retry preset"},
		"unparseable":     {"1s,banana", "retry delay 2"},
		"negative":        {"1s,-2s", "negative"},
		"trailing comma":  {"1s,", "retry delay 2 is empty"},
		"bare number":     {"5", "missing unit"},
		"leading empty":   {",1s", "retry delay 1 is empty"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseRetrySchedule(tc.spec, false)
			if err == nil {
				t.Fatalf("ParseRetrySchedule(%q) succeeded, want an error", tc.spec)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

// An unknown preset must not be reported as a duration parse failure -- that
// sends the reader looking in the wrong place entirely.
func TestUnknownPresetNamesTheAlternatives(t *testing.T) {
	t.Parallel()

	_, err := ParseRetrySchedule("aggressive", false)
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"demo", "standard"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name the %q preset", err, want)
		}
	}
}

func TestValidateLiveness(t *testing.T) {
	t.Parallel()

	demo, err := ParseRetrySchedule("demo", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// 15s against kafka-go's 30s default rebalance timeout.
	if err := demo.ValidateLiveness(30 * time.Second); err != nil {
		t.Errorf("demo preset rejected against a 30s rebalance timeout: %v", err)
	}

	std, err := ParseRetrySchedule("standard", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// The whole point: the production schedule cannot run in process, and that
	// has to surface at startup rather than as a mid-retry reassignment.
	err = std.ValidateLiveness(30 * time.Second)
	if err == nil {
		t.Fatal("standard preset accepted against a 30s rebalance timeout, want rejection")
	}
	if !strings.Contains(err.Error(), "rebalance timeout") {
		t.Errorf("rejection %q should explain the rebalance timeout", err)
	}
}

// Equal to the timeout is already too long: a consumer that wakes exactly as
// the coordinator gives up has still missed it.
func TestValidateLivenessBoundaryIsExclusive(t *testing.T) {
	t.Parallel()

	s, err := ParseRetrySchedule("10s,10s,10s", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := s.ValidateLiveness(30 * time.Second); err == nil {
		t.Error("a 30s schedule was accepted against a 30s rebalance timeout, want rejection")
	}
	if err := s.ValidateLiveness(31 * time.Second); err != nil {
		t.Errorf("a 30s schedule was rejected against a 31s rebalance timeout: %v", err)
	}
}

func TestDelayForBounds(t *testing.T) {
	t.Parallel()

	s, err := ParseRetrySchedule("1s,2s,4s", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	for _, retry := range []int{0, -1, 4, 99} {
		if _, ok := s.DelayFor(retry); ok {
			t.Errorf("DelayFor(%d) returned ok, want the budget to be spent", retry)
		}
	}
	for retry, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second} {
		got, ok := s.DelayFor(retry)
		if !ok {
			t.Fatalf("DelayFor(%d) not ok", retry)
		}
		if got != want {
			t.Errorf("DelayFor(%d) = %s, want %s", retry, got, want)
		}
	}
}

// Equal jitter: never below half the delay, never above the delay itself. Full
// jitter would allow a near-instant retry against a subscriber still down.
func TestJitterStaysWithinHalfAndWhole(t *testing.T) {
	t.Parallel()

	s := RetrySchedule{Name: "custom", Delays: []time.Duration{10 * time.Second}, Jitter: true}
	for _, frac := range []float64{0, 0.25, 0.5, 0.75, 0.999999} {
		got := s.jittered(10*time.Second, frac)
		if got < 5*time.Second || got >= 10*time.Second {
			t.Errorf("jittered(10s, %v) = %s, want [5s, 10s)", frac, got)
		}
	}

	off := RetrySchedule{Name: "custom", Delays: []time.Duration{10 * time.Second}}
	if got := off.jittered(10*time.Second, 0.9); got != 10*time.Second {
		t.Errorf("jitter disabled: got %s, want exactly 10s", got)
	}
}

func TestEmptyScheduleIsSentinel(t *testing.T) {
	t.Parallel()

	if _, err := ParseRetrySchedule("", false); !errors.Is(err, ErrEmptySchedule) {
		t.Errorf("empty spec error = %v, want ErrEmptySchedule", err)
	}
	if err := (RetrySchedule{}).ValidateLiveness(time.Minute); !errors.Is(err, ErrEmptySchedule) {
		t.Errorf("zero schedule error = %v, want ErrEmptySchedule", err)
	}
}

// The startup log line has to state the budget, not imply it.
func TestStringStatesTheBudget(t *testing.T) {
	t.Parallel()

	s, err := ParseRetrySchedule("demo", false)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got := s.String()
	for _, want := range []string{"demo", "5 attempts", "15s"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, want it to mention %q", got, want)
		}
	}
}

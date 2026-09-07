package relayverify

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

const (
	defaultOrderingIngestURL = "http://localhost:8082"
	defaultOrderingEvents    = 40
	defaultOrderingTenant    = "globex"
	orderingTimeout          = 60 * time.Second
	orderingPollInterval     = time.Second
)

// OrderingOptions controls one steady-state ordering verification.
type OrderingOptions struct {
	Events       int
	Tenant       string
	Timeout      time.Duration
	PollInterval time.Duration
	HTTPClient   *http.Client
	Output       io.Writer
}

// ParseOrderingOptions reads flags with EVENTS and TENANT as their fallbacks.
func ParseOrderingOptions(args []string) (OrderingOptions, error) {
	events := defaultOrderingEvents
	if raw := os.Getenv("EVENTS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return OrderingOptions{}, fmt.Errorf("EVENTS must be a positive integer: %q", raw)
		}
		events = parsed
	}
	tenant := os.Getenv("TENANT")
	if tenant == "" {
		tenant = defaultOrderingTenant
	}

	opts := OrderingOptions{
		Events:       events,
		Tenant:       tenant,
		Timeout:      orderingTimeout,
		PollInterval: orderingPollInterval,
		HTTPClient:   http.DefaultClient,
		Output:       os.Stdout,
	}
	flags := flag.NewFlagSet("ordering", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.IntVar(&opts.Events, "events", opts.Events, "number of events to post")
	flags.StringVar(&opts.Tenant, "tenant", opts.Tenant, "tenant whose delivery order to verify")
	if err := flags.Parse(args); err != nil {
		return OrderingOptions{}, err
	}
	if flags.NArg() != 0 {
		return OrderingOptions{}, fmt.Errorf("ordering takes no positional arguments")
	}
	if opts.Events <= 0 {
		return OrderingOptions{}, fmt.Errorf("events must be a positive integer")
	}
	if opts.Tenant == "" {
		return OrderingOptions{}, fmt.Errorf("tenant must not be empty")
	}
	return opts, nil
}

// Ordering proves that one tenant's events reach the sink in acceptance order
// with one steady-state consumer.
func Ordering(ctx context.Context, cfg platform.Config, opts OrderingOptions) error {
	if opts.Events <= 0 {
		return fmt.Errorf("events must be a positive integer")
	}
	if opts.Tenant == "" {
		return fmt.Errorf("tenant must not be empty")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = orderingTimeout
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = orderingPollInterval
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}
	if opts.Output == nil {
		opts.Output = io.Discard
	}

	ingestURL := strings.TrimSuffix(cfg.RelayIngestURL, "/")
	if ingestURL == "" {
		// Match ${RELAY_INGEST_URL:-http://localhost:8082} in the shell
		// reference. platform.Load preserves an explicit empty value because it
		// disables the relay component in the general smoke program, but this
		// dedicated verifier always requires relay.
		ingestURL = defaultOrderingIngestURL
	}
	sink := SinkClient{BaseURL: cfg.SinkURL, HTTPClient: opts.HTTPClient}

	say(opts.Output, "checking relay and the sink are up")
	if err := requireOK(ctx, opts.HTTPClient, ingestURL+"/readyz"); err != nil {
		return fmt.Errorf("relay ingest is not reachable at %s -- run 'make up-apps': %w",
			ingestURL, err)
	}
	if err := sink.Health(ctx); err != nil {
		return fmt.Errorf("the sink is not reachable at %s -- run 'make up-apps': %w", cfg.SinkURL, err)
	}

	// A sink still slowed by an earlier demo would make this time out for a
	// reason unrelated to ordering.
	say(opts.Output, "resetting sink latency and failure rate")
	if err := sink.ResetControl(ctx); err != nil {
		return fmt.Errorf("could not reset the sink through %s/control: %w", cfg.SinkURL, err)
	}
	say(opts.Output, "clearing the sink's delivery history")
	if err := sink.ClearReceived(ctx); err != nil {
		return fmt.Errorf("could not clear %s/received: %w", cfg.SinkURL, err)
	}

	// globex is the default because acme has a second subscription at
	// /hooks/flaky. Relay commits only after every subscriber reaches a terminal
	// state, so acme would measure the retry schedule instead of ordering.
	marker := fmt.Sprintf("ordering-%d-%d", time.Now().UnixNano(), os.Getpid())

	// Each POST waits for its 202 before the next begins. Concurrent requests
	// would leave the accepted order undefined.
	say(opts.Output, "posting %d events for tenant %s, one at a time", opts.Events, opts.Tenant)
	for sequence := 1; sequence <= opts.Events; sequence++ {
		if err := postOrderingEvent(ctx, opts.HTTPClient, ingestURL, opts.Tenant, marker, sequence); err != nil {
			return err
		}
	}

	say(opts.Output, "waiting for all %d to reach the sink", opts.Events)
	delivered, err := awaitSequences(
		ctx, sink, marker, opts.Events, opts.Timeout, opts.PollInterval,
	)
	if err != nil {
		return err
	}

	say(opts.Output, "asserting delivery order matches accepted order")
	if err := compareOrder(delivered, opts.Events); err != nil {
		_, _ = fmt.Fprintf(opts.Output, "  %v\n", err)
		_, _ = fmt.Fprintf(opts.Output, "  delivered: %s\n", formatSequence(delivered))
		return fmt.Errorf("tenant %s received its events out of order", opts.Tenant)
	}

	pass(opts.Output, "%d events for %s delivered in the order they were accepted", opts.Events, opts.Tenant)
	_, _ = fmt.Fprintln(opts.Output)
	_, _ = fmt.Fprintln(opts.Output, "  Steady state only: one consumer, no membership change. Ordering across a")
	_, _ = fmt.Fprintln(opts.Output, "  rebalance is the other half of issue #54: run")
	_, _ = fmt.Fprintln(opts.Output, "  scripts/verify-ordering-rebalance.sh, which starts a second consumer and")
	_, _ = fmt.Fprintln(opts.Output, "  waits for the broker to report the group has actually changed generation.")
	return nil
}

func requireOK(ctx context.Context, client *http.Client, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxSinkResponse))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %d, want 200", url, resp.StatusCode)
	}
	return nil
}

func postOrderingEvent(
	ctx context.Context,
	client *http.Client,
	ingestURL string,
	tenant string,
	marker string,
	sequence int,
) error {
	body, err := json.Marshal(map[string]any{
		"tenant_id": tenant,
		"type":      "ordering.check",
		"data": map[string]any{
			"seq":    sequence,
			"marker": marker,
		},
	})
	if err != nil {
		return fmt.Errorf("encode event %d: %w", sequence, err)
	}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, ingestURL+"/v1/events", bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("build event %d request: %w", sequence, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post event %d: %w", sequence, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxSinkResponse))
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("ingest returned %d for event %d, expected 202", resp.StatusCode, sequence)
	}
	return nil
}

func awaitSequences(
	ctx context.Context,
	sink SinkClient,
	marker string,
	want int,
	timeout time.Duration,
	pollInterval time.Duration,
) ([]int, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var sequences []int
	for {
		deliveries, err := sink.Received(pollCtx)
		if err != nil {
			if pollCtx.Err() != nil {
				return timeoutError(ctx, sequences, want, timeout)
			}
			return nil, fmt.Errorf("read sink deliveries: %w", err)
		}
		sequences = extractSequences(deliveries, marker)
		if len(sequences) >= want {
			return sequences, nil
		}

		select {
		case <-pollCtx.Done():
			return timeoutError(ctx, sequences, want, timeout)
		case <-ticker.C:
		}
	}
}

func timeoutError(parent context.Context, sequences []int, want int, timeout time.Duration) ([]int, error) {
	if err := parent.Err(); err != nil {
		return sequences, err
	}
	return sequences, fmt.Errorf("only %d of %d were delivered within %s", len(sequences), want, timeout)
}

// extractSequences filters another run's traffic and returns this run's
// successful /hooks/ok deliveries in sink order.
func extractSequences(deliveries []Delivery, marker string) []int {
	sequences := make([]int, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.Path != "/hooks/ok" || delivery.Status < 200 || delivery.Status >= 300 {
			continue
		}
		var gotMarker string
		if err := json.Unmarshal(delivery.Data["marker"], &gotMarker); err != nil || gotMarker != marker {
			continue
		}
		var sequence int
		if err := json.Unmarshal(delivery.Data["seq"], &sequence); err != nil {
			continue
		}
		sequences = append(sequences, sequence)
	}
	return sequences
}

// compareOrder reports the first divergence from 1..want.
func compareOrder(delivered []int, want int) error {
	limit := min(len(delivered), want)
	for position := 0; position < limit; position++ {
		expected := position + 1
		if delivered[position] != expected {
			return fmt.Errorf("position %d: delivered seq %d, expected %d",
				position, delivered[position], expected)
		}
	}
	if len(delivered) != want {
		return fmt.Errorf("lengths differ: %d delivered, %d expected", len(delivered), want)
	}
	return nil
}

func formatSequence(sequence []int) string {
	parts := make([]string, len(sequence))
	for i, value := range sequence {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, " ")
}

func say(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, "\x1b[1;34m==>\x1b[0m "+format+"\n", args...)
}

func pass(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, "\x1b[32mPASS\x1b[0m "+format+"\n", args...)
}

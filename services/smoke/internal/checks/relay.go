package checks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

// Relay drives the whole webhook pipeline: it POSTs an event to relay's ingest
// endpoint, waits for the sink to receive it, and asserts the payload came back
// unchanged.
//
// The seeded tenant `acme` has two subscribers -- one healthy and one that
// always fails -- so a single event exercises both outcomes. The check asserts
// the healthy delivery and then that the failing one was dead-lettered, which
// is the part a unit test with fakes cannot prove: that the record actually
// reached the DLQ topic on the broker.
//
// The DLQ read records the topic's end offset *before* publishing and starts
// there, so it never scans history. Consuming from the earliest offset is what
// made the kafka check O(topic size) and time out at 60,001 messages; see
// issue #1 and ADR 0004.
func Relay(cfg platform.Config) Check {
	return Check{Name: "relay", Run: func(ctx context.Context) (string, error) {
		if cfg.RelayIngestURL == "" {
			return "skipped (apps profile disabled)", nil
		}

		marker := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
		brokers := strings.Split(cfg.KafkaBrokers, ",")
		relayStarts, err := topicEndOffsets(ctx, brokers[0], cfg.RelayTopic)
		if err != nil {
			return "", fmt.Errorf("read %s end offsets: %w", cfg.RelayTopic, err)
		}

		// Where the DLQ ends now. Anything at or after this offset belongs to
		// this run.
		dlqStart, err := endOffset(ctx, brokers[0], cfg.RelayDLQTopic, 0)
		if err != nil {
			return "", fmt.Errorf("read %s end offset: %w", cfg.RelayDLQTopic, err)
		}

		eventIDs := make([]string, 2)
		postErrors := make([]error, 2)
		start := make(chan struct{})
		var posts sync.WaitGroup
		for i := range eventIDs {
			posts.Add(1)
			go func() {
				defer posts.Done()
				<-start
				eventIDs[i], postErrors[i] = postEvent(ctx, cfg.RelayIngestURL, marker, marker)
			}()
		}
		close(start)
		posts.Wait()
		for i, err := range postErrors {
			if err != nil {
				return "", fmt.Errorf("concurrent idempotent event %d: %w", i+1, err)
			}
		}
		eventID := eventIDs[0]
		if eventIDs[1] != eventID {
			return "", fmt.Errorf("concurrent idempotent event ids differ: %s and %s", eventID, eventIDs[1])
		}
		if err := checkDurableIdempotencyClaim(ctx, cfg.DatabaseURL, "acme", marker, eventID); err != nil {
			return "", err
		}
		published, err := countEventRecords(ctx, brokers, cfg.RelayTopic, relayStarts, eventID)
		if err != nil {
			return "", err
		}
		if published != 1 {
			return "", fmt.Errorf("event %s appeared %d times on %s after two concurrent requests, want 1",
				eventID, published, cfg.RelayTopic)
		}

		delivered, err := awaitDelivery(ctx, cfg.SinkURL, eventID, marker)
		if err != nil {
			return "", err
		}

		dead, err := awaitDeadLetter(ctx, brokers, cfg.RelayDLQTopic, dlqStart, eventID)
		if err != nil {
			return "", err
		}
		attempts, err := fetchAttemptHistory(ctx, cfg.RelayIngestURL, eventID)
		if err != nil {
			return "", err
		}
		if err := checkAttemptHistory(eventID, attempts); err != nil {
			return "", err
		}
		healthyDeliveries, err := countHealthyDeliveries(ctx, cfg.SinkURL, eventID)
		if err != nil {
			return "", err
		}
		if healthyDeliveries != 1 {
			return "", fmt.Errorf("event %s reached /hooks/ok %d times, want 1", eventID, healthyDeliveries)
		}
		traceEvidence := "Tempo assertion off (set MLP_SMOKE_REQUIRE_TRACES=1, or run `make smoke-traces`, with the obs profile up)"
		if cfg.RequireTraces {
			traceID := trace.SpanContextFromContext(ctx).TraceID()
			if !traceID.IsValid() {
				return "", fmt.Errorf("MLP_SMOKE_REQUIRE_TRACES is set but this run has no trace id: " +
					"the OTLP exporter did not start, so no relay span can be looked up")
			}
			if err := awaitRelayTrace(ctx, cfg.TempoURL, traceID.String(), eventID, attempts); err != nil {
				return "", err
			}
			traceEvidence = fmt.Sprintf(
				"Tempo trace %s joins ingest, Kafka, consume and one span per persisted attempt", traceID)
		}

		return fmt.Sprintf("%s returned by 2 concurrent requests for one idempotency key, delivered to %s, dead-lettered %s after %d attempts; one Kafka record, one healthy delivery, one event row and %d attempts persisted; %s",
			eventID, delivered, dead.URL, dead.Attempts, len(attempts), traceEvidence), nil
	}}
}

// postEvent submits one event and returns the id relay assigned it.
func postEvent(ctx context.Context, ingestURL, marker, idempotencyKey string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"tenant_id":       "acme",
		"type":            "smoke.check",
		"data":            map[string]string{"marker": marker},
		"idempotency_key": idempotencyKey,
	})
	if err != nil {
		return "", fmt.Errorf("encode event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimSuffix(ingestURL, "/")+"/v1/events", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("post event: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode != http.StatusAccepted {
		return "", fmt.Errorf("ingest returned %d, want 202: %s", resp.StatusCode, bytes.TrimSpace(payload))
	}

	var accepted struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &accepted); err != nil {
		return "", fmt.Errorf("decode ingest response %q: %w", payload, err)
	}
	if accepted.ID == "" {
		return "", fmt.Errorf("ingest returned 202 with no event id: %s", payload)
	}
	return accepted.ID, nil
}

// tempoSpan is the subset of Tempo's OTLP-JSON trace response this check
// reads. Integers arrive as decimal strings in OTLP JSON, hence intValue being
// a string rather than a number.
type tempoSpan struct {
	Name       string `json:"name"`
	Attributes []struct {
		Key   string `json:"key"`
		Value struct {
			StringValue string `json:"stringValue"`
			IntValue    string `json:"intValue"`
		} `json:"value"`
	} `json:"attributes"`
}

type tempoTrace struct {
	Batches []struct {
		ScopeSpans []struct {
			Spans []tempoSpan `json:"spans"`
		} `json:"scopeSpans"`
	} `json:"batches"`
}

// attemptKey identifies one webhook attempt the way both the persisted history
// row and its span do.
type attemptKey struct {
	SubscriptionID int64
	AttemptNumber  int
}

// relayTraceSpans is what one Tempo response says about this event.
type relayTraceSpans struct {
	counts   map[string]int
	attempts map[attemptKey]int
}

func (s relayTraceSpans) String() string {
	return fmt.Sprintf("ingest=%d produce=%d consume=%d attempt spans=%v",
		s.counts["relay.ingest"], s.counts["kafka.produce"], s.counts["relay.consume"], s.attempts)
}

// awaitRelayTrace polls until one Tempo trace holds the whole relay path for
// this event, or the check's deadline expires.
//
// The event id is what ties the spans together, and the trace id is the smoke
// run's own: relay.consume is created in a different process and can only carry
// this trace id if it came across in the Kafka headers, so finding it here is
// the proof that the header hop works.
//
// wantAttempts is the persisted attempt history. Asserting the exact set of
// (subscription, attempt number) pairs rather than "at least one attempt span"
// is what makes this fail if per-attempt spans ever collapse into one -- which
// is the regression an "every delivery attempt" criterion most needs to catch,
// and the one a name-presence check cannot see.
func awaitRelayTrace(ctx context.Context, tempoURL, traceID, eventID string, wantAttempts []deliveryAttempt) error {
	want := make(map[attemptKey]int, len(wantAttempts))
	for _, attempt := range wantAttempts {
		want[attemptKey{attempt.SubscriptionID, attempt.AttemptNumber}]++
	}
	if len(want) == 0 {
		return fmt.Errorf("event %s persisted no delivery attempts, so there is nothing to match spans against", eventID)
	}

	var last relayTraceSpans
	var lastErr error
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, err := fetchRelayTrace(ctx, tempoURL, traceID, eventID)
		lastErr = err
		if err == nil {
			last = got
			if missing := missingRelaySpans(got, want); missing == "" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("tempo trace %s for event %s was never readable (last error: %v): %w",
					traceID, eventID, lastErr, ctx.Err())
			}
			return fmt.Errorf("tempo trace %s is incomplete for event %s: %s; saw %s: %w",
				traceID, eventID, missingRelaySpans(last, want), last, ctx.Err())
		case <-ticker.C:
		}
	}
}

// missingRelaySpans reports what the trace still lacks, or "" when it is
// complete.
func missingRelaySpans(got relayTraceSpans, want map[attemptKey]int) string {
	for _, name := range []string{"relay.ingest", "kafka.produce", "relay.consume"} {
		if got.counts[name] == 0 {
			return "no " + name + " span"
		}
	}
	for key, n := range want {
		if got.attempts[key] != n {
			return fmt.Sprintf("subscription %d attempt %d has %d spans, want %d",
				key.SubscriptionID, key.AttemptNumber, got.attempts[key], n)
		}
	}
	for key := range got.attempts {
		if _, ok := want[key]; !ok {
			return fmt.Sprintf("subscription %d attempt %d has a span but no persisted history row",
				key.SubscriptionID, key.AttemptNumber)
		}
	}
	return ""
}

func fetchRelayTrace(ctx context.Context, tempoURL, traceID, eventID string) (relayTraceSpans, error) {
	out := relayTraceSpans{counts: map[string]int{}, attempts: map[attemptKey]int{}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(tempoURL, "/")+"/api/traces/"+traceID, nil)
	if err != nil {
		return out, fmt.Errorf("build Tempo trace request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return out, fmt.Errorf("query Tempo trace: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8192))
		return out, fmt.Errorf("tempo trace query returned %d", resp.StatusCode)
	}
	var result tempoTrace
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return out, fmt.Errorf("decode Tempo trace: %w", err)
	}

	for _, batch := range result.Batches {
		for _, scope := range batch.ScopeSpans {
			for _, span := range scope.Spans {
				if !relaySpanHasEvent(span, eventID) {
					continue
				}
				out.counts[span.Name]++
				if span.Name != "relay.webhook.attempt" {
					continue
				}
				subscription, okSub := span.intAttr("relay.subscription.id")
				attempt, okAttempt := span.intAttr("relay.delivery.attempt")
				if !okSub || !okAttempt {
					continue
				}
				out.attempts[attemptKey{subscription, int(attempt)}]++
			}
		}
	}
	return out, nil
}

func relaySpanHasEvent(span tempoSpan, eventID string) bool {
	switch span.Name {
	case "relay.ingest", "kafka.produce", "relay.consume", "relay.webhook.attempt":
	default:
		return false
	}
	value, ok := span.stringAttr("relay.event.id")
	return ok && value == eventID
}

func (s tempoSpan) stringAttr(key string) (string, bool) {
	for _, attr := range s.Attributes {
		if attr.Key == key {
			return attr.Value.StringValue, true
		}
	}
	return "", false
}

func (s tempoSpan) intAttr(key string) (int64, bool) {
	for _, attr := range s.Attributes {
		if attr.Key != key {
			continue
		}
		n, err := strconv.ParseInt(attr.Value.IntValue, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

func checkDurableIdempotencyClaim(
	ctx context.Context,
	databaseURL, tenantID, idempotencyKey, eventID string,
) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect for idempotency check: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var (
		count   int
		matches bool
	)
	if err := conn.QueryRow(ctx, `
		SELECT count(*), COALESCE(bool_and(id = $3 AND published_at IS NOT NULL), false)
		FROM relay_events
		WHERE tenant_id = $1 AND idempotency_key = $2
		  AND idempotency_claimed_at IS NOT NULL`,
		tenantID, idempotencyKey, eventID,
	).Scan(&count, &matches); err != nil {
		return fmt.Errorf("query idempotency claim: %w", err)
	}
	if count != 1 || !matches {
		return fmt.Errorf("idempotency claim count=%d matches accepted event=%t, want one published row for %s",
			count, matches, eventID)
	}
	return nil
}

type deliveryAttempt struct {
	SubscriptionID  int64  `json:"subscription_id"`
	SubscriptionURL string `json:"subscription_url"`
	AttemptNumber   int    `json:"attempt_number"`
	Outcome         string `json:"outcome"`
}

func fetchAttemptHistory(ctx context.Context, ingestURL, eventID string) ([]deliveryAttempt, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(ingestURL, "/")+"/v1/events/"+eventID+"/attempts", nil)
	if err != nil {
		return nil, fmt.Errorf("build attempt-history request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query attempt history: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("attempt history returned %d, want 200: %s",
			resp.StatusCode, bytes.TrimSpace(payload))
	}
	var got struct {
		EventID  string            `json:"event_id"`
		Attempts []deliveryAttempt `json:"attempts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		return nil, fmt.Errorf("decode attempt history: %w", err)
	}
	if got.EventID != eventID {
		return nil, fmt.Errorf("attempt history event id %q, want %q", got.EventID, eventID)
	}
	return got.Attempts, nil
}

func checkAttemptHistory(eventID string, attempts []deliveryAttempt) error {
	if len(attempts) != 4 {
		return fmt.Errorf("event %s has %d attempts, want exactly 4", eventID, len(attempts))
	}
	var delivered, exhausted bool
	lastBySubscription := make(map[int64]int)
	for _, attempt := range attempts {
		if attempt.SubscriptionID <= 0 || attempt.SubscriptionURL == "" {
			return fmt.Errorf("event %s has an attempt without subscriber identity: %+v", eventID, attempt)
		}
		if attempt.AttemptNumber <= lastBySubscription[attempt.SubscriptionID] {
			return fmt.Errorf("event %s subscription %d attempt number %d follows %d",
				eventID, attempt.SubscriptionID, attempt.AttemptNumber,
				lastBySubscription[attempt.SubscriptionID])
		}
		lastBySubscription[attempt.SubscriptionID] = attempt.AttemptNumber
		if strings.HasSuffix(attempt.SubscriptionURL, "/hooks/ok") && attempt.Outcome == "delivered" {
			delivered = true
		}
		if strings.HasSuffix(attempt.SubscriptionURL, "/hooks/flaky") && attempt.Outcome == "exhausted" {
			exhausted = true
		}
	}
	if !delivered || !exhausted {
		return fmt.Errorf("event %s history did not contain delivered /hooks/ok and exhausted /hooks/flaky attempts: %+v",
			eventID, attempts)
	}
	return nil
}

type sinkDelivery struct {
	Path      string          `json:"path"`
	WebhookID string          `json:"webhook_id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Status    int             `json:"status"`
}

// awaitDelivery polls the sink until the healthy subscriber has been given this
// event, and checks the payload survived the trip. It returns the path it
// arrived on.
//
// No deadline of its own: Run already bounds every check, and a second timeout
// here would be one more number to keep in agreement with the first.
func awaitDelivery(ctx context.Context, sinkURL, eventID, marker string) (string, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		deliveries, err := fetchDeliveries(ctx, sinkURL)
		if err != nil {
			return "", err
		}
		for _, d := range deliveries {
			if d.WebhookID != eventID || d.Status < 200 || d.Status > 299 {
				continue
			}
			// Arrived. Now prove it is the payload that was sent, not just
			// something with the right id.
			var data struct {
				Marker string `json:"marker"`
			}
			if err := json.Unmarshal(d.Data, &data); err != nil {
				return "", fmt.Errorf("delivered payload is not decodable: %w", err)
			}
			if data.Marker != marker {
				return "", fmt.Errorf("delivered marker %q, want %q", data.Marker, marker)
			}
			if d.Type != "smoke.check" {
				return "", fmt.Errorf("delivered type %q, want %q", d.Type, "smoke.check")
			}
			return d.Path, nil
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return "", fmt.Errorf("event %s was not delivered to the sink: %w", eventID, ctx.Err())
		}
	}
}

func fetchDeliveries(ctx context.Context, sinkURL string) ([]sinkDelivery, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		strings.TrimSuffix(sinkURL, "/")+"/received", nil)
	if err != nil {
		return nil, fmt.Errorf("build sink request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query sink: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("sink returned %d, want 200", resp.StatusCode)
	}
	var got struct {
		Deliveries []sinkDelivery `json:"deliveries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		return nil, fmt.Errorf("decode sink response: %w", err)
	}
	return got.Deliveries, nil
}

func countHealthyDeliveries(ctx context.Context, sinkURL, eventID string) (int, error) {
	deliveries, err := fetchDeliveries(ctx, sinkURL)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, delivery := range deliveries {
		if delivery.WebhookID == eventID && strings.HasSuffix(delivery.Path, "/hooks/ok") &&
			delivery.Status >= 200 && delivery.Status <= 299 {
			count++
		}
	}
	return count, nil
}

type deadLetter struct {
	Record struct {
		ID string `json:"id"`
	} `json:"record"`
	URL      string `json:"url"`
	Attempts int    `json:"attempts"`
	Reason   string `json:"reason"`
}

// awaitDeadLetter reads forward from start until it finds this event's failed
// delivery, proving the DLQ topic is really written rather than merely intended.
func awaitDeadLetter(ctx context.Context, brokers []string, topic string, start int64, eventID string) (deadLetter, error) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		// Close blocks on the fetch in flight, and once this check has what it
		// came for the next fetch has nothing to return. See ADR 0004.
		MaxWait: 250 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()

	if err := r.SetOffset(start); err != nil {
		return deadLetter{}, fmt.Errorf("seek %s to %d: %w", topic, start, err)
	}

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return deadLetter{}, fmt.Errorf("no dead letter for %s on %s: %w", eventID, topic, err)
		}
		var dl deadLetter
		if err := json.Unmarshal(m.Value, &dl); err != nil {
			// Another run's record, or something malformed. Keep looking
			// rather than failing on a record this check did not write.
			continue
		}
		if dl.Record.ID == eventID {
			if dl.Reason == "" {
				return dl, fmt.Errorf("dead letter for %s carries no reason", eventID)
			}
			return dl, nil
		}
	}
}

// endOffset reports where a partition currently ends, so a reader can start
// after everything already on the topic.
func endOffset(ctx context.Context, broker, topic string, partition int) (int64, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", broker, topic, partition)
	if err != nil {
		return 0, fmt.Errorf("dial leader: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return conn.ReadLastOffset()
}

func topicEndOffsets(ctx context.Context, broker, topic string) (map[int]int64, error) {
	partitions, err := kafka.LookupPartitions(ctx, "tcp", broker, topic)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	offsets := make(map[int]int64, len(partitions))
	for _, partition := range partitions {
		if _, seen := offsets[partition.ID]; seen {
			continue
		}
		offset, err := endOffset(ctx, broker, topic, partition.ID)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", partition.ID, err)
		}
		offsets[partition.ID] = offset
	}
	if len(offsets) == 0 {
		return nil, fmt.Errorf("topic has no partitions")
	}
	return offsets, nil
}

func countEventRecords(
	ctx context.Context,
	brokers []string,
	topic string,
	starts map[int]int64,
	eventID string,
) (int, error) {
	ends, err := topicEndOffsets(ctx, brokers[0], topic)
	if err != nil {
		return 0, fmt.Errorf("read %s final offsets: %w", topic, err)
	}

	count := 0
	for partition, start := range starts {
		end, ok := ends[partition]
		if !ok {
			return 0, fmt.Errorf("%s partition %d disappeared", topic, partition)
		}
		if end < start {
			return 0, fmt.Errorf("%s partition %d end offset moved from %d to %d", topic, partition, start, end)
		}
		if end == start {
			continue
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: topic, Partition: partition,
			MinBytes: 1, MaxBytes: 10e6, MaxWait: 250 * time.Millisecond,
		})
		if err := reader.SetOffset(start); err != nil {
			_ = reader.Close()
			return 0, fmt.Errorf("seek %s partition %d to %d: %w", topic, partition, start, err)
		}
		for offset := start; offset < end; offset++ {
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				_ = reader.Close()
				return 0, fmt.Errorf("read %s partition %d offset %d: %w", topic, partition, offset, err)
			}
			var record struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(message.Value, &record) == nil && record.ID == eventID {
				count++
			}
		}
		if err := reader.Close(); err != nil {
			return 0, fmt.Errorf("close %s partition %d reader: %w", topic, partition, err)
		}
	}
	return count, nil
}

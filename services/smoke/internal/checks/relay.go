package checks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

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
			return "", errRelayDisabled
		}

		marker := fmt.Sprintf("smoke-%d", time.Now().UnixNano())
		brokers := strings.Split(cfg.KafkaBrokers, ",")

		// Where the DLQ ends now. Anything at or after this offset belongs to
		// this run.
		dlqStart, err := endOffset(ctx, brokers[0], cfg.RelayDLQTopic, 0)
		if err != nil {
			return "", fmt.Errorf("read %s end offset: %w", cfg.RelayDLQTopic, err)
		}

		eventID, err := postEvent(ctx, cfg.RelayIngestURL, marker)
		if err != nil {
			return "", err
		}

		delivered, err := awaitDelivery(ctx, cfg.SinkURL, eventID, marker)
		if err != nil {
			return "", err
		}

		dead, err := awaitDeadLetter(ctx, brokers, cfg.RelayDLQTopic, dlqStart, eventID)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("%s delivered to %s, dead-lettered %s after %d attempts",
			eventID, delivered, dead.URL, dead.Attempts), nil
	}}
}

var errRelayDisabled = fmt.Errorf("RELAY_INGEST_URL is empty; start the apps profile with `make up-apps`")

// postEvent submits one event and returns the id relay assigned it.
func postEvent(ctx context.Context, ingestURL, marker string) (string, error) {
	body, err := json.Marshal(map[string]any{
		"tenant_id": "acme",
		"type":      "smoke.check",
		"data":      map[string]string{"marker": marker},
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

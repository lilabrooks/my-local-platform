// Package relayverify holds stateful relay verification harnesses that run
// separately from the component smoke checks.
package relayverify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxSinkResponse = 8192

// Delivery is one retained request reported by the sink.
type Delivery struct {
	ReceivedAt time.Time
	Path       string
	Data       map[string]json.RawMessage
	Status     int
}

// UnmarshalJSON accepts the two data shapes used by the shell verification:
// a nested JSON object, and a string containing that object. Invalid data stays
// nil so a caller can ignore another run's malformed delivery without losing
// the rest of the response.
func (d *Delivery) UnmarshalJSON(input []byte) error {
	var wire struct {
		ReceivedAt time.Time       `json:"received_at"`
		Path       string          `json:"path"`
		Data       json.RawMessage `json:"data"`
		Status     int             `json:"status"`
	}
	if err := json.Unmarshal(input, &wire); err != nil {
		return err
	}

	d.ReceivedAt = wire.ReceivedAt
	d.Path = wire.Path
	d.Status = wire.Status
	d.Data = decodeDeliveryData(wire.Data)
	return nil
}

func decodeDeliveryData(raw json.RawMessage) map[string]json.RawMessage {
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil {
		raw = json.RawMessage(encoded)
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil
	}
	return object
}

// SinkClient is the subset of the sink HTTP surface shared by relay
// verification harnesses.
type SinkClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

func (c SinkClient) client() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// Health verifies that the sink is reachable.
func (c SinkClient) Health(ctx context.Context) error {
	return c.request(ctx, http.MethodGet, "/healthz", nil, http.StatusOK, nil)
}

// ResetControl removes latency and failures left by an earlier demo.
func (c SinkClient) ResetControl(ctx context.Context) error {
	body := bytes.NewBufferString(`{"latency_ms":0,"fail_rate":0}`)
	return c.request(ctx, http.MethodPost, "/control", body, http.StatusOK, nil)
}

// ClearReceived removes the sink's retained delivery history.
func (c SinkClient) ClearReceived(ctx context.Context) error {
	return c.request(ctx, http.MethodDelete, "/received", nil, http.StatusNoContent, nil)
}

// Received returns retained deliveries in the order the sink received them.
func (c SinkClient) Received(ctx context.Context) ([]Delivery, error) {
	var response struct {
		Deliveries []Delivery `json:"deliveries"`
	}
	if err := c.request(ctx, http.MethodGet, "/received", nil, http.StatusOK, &response); err != nil {
		return nil, err
	}
	return response.Deliveries, nil
}

func (c SinkClient) request(
	ctx context.Context,
	method string,
	path string,
	body io.Reader,
	wantStatus int,
	destination any,
) error {
	url := strings.TrimSuffix(c.BaseURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("build sink request %s %s: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client().Do(req)
	if err != nil {
		return fmt.Errorf("sink request %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != wantStatus {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, maxSinkResponse))
		return fmt.Errorf("sink %s %s returned %d, want %d: %s",
			method, path, resp.StatusCode, wantStatus, bytes.TrimSpace(payload))
	}
	if destination == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxSinkResponse))
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(destination); err != nil {
		return fmt.Errorf("decode sink %s: %w", path, err)
	}
	return nil
}

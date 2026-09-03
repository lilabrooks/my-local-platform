// Package event defines what relay puts on the log and what it sends to a
// subscriber. They are deliberately not the same shape.
//
// The goal document left this open: whether the `type` + `data` envelope
// Standard Webhooks recommends should also be the Kafka record's shape.
// It should not. The record carries routing metadata the delivery consumer
// needs -- tenant, event id, when it happened -- and none of that belongs in a
// subscriber's request body. Coupling them would make the log's schema an
// external contract, so every field the consumer ever needs would become a
// field subscribers can see and depend on.
//
// So: Record is internal and may grow. Payload is what subscribers receive and
// is exactly what the specification describes.
package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// MaxDataBytes caps a single event's payload. A webhook body is a notification,
// not a transport for bulk data, and an unbounded field here becomes an
// unbounded Kafka record and an unbounded HTTP request to a subscriber.
const MaxDataBytes = 256 * 1024

// Record is one event on `mlp.relay.deliveries`. Internal; may gain fields.
type Record struct {
	// ID is sent to subscribers as `webhook-id` and doubles as their
	// idempotency key, so it must be stable across every retry.
	ID string `json:"id"`
	// TenantID is also the partition key, which is what buys per-tenant
	// ordering.
	TenantID string `json:"tenant_id"`
	// Type is the event type, e.g. "invoice.paid".
	Type string `json:"type"`
	// Data is the event body, passed through unmodified.
	Data json.RawMessage `json:"data"`
	// OccurredAt is when relay accepted the event, not when the subscriber is
	// reached.
	OccurredAt time.Time `json:"occurred_at"`
	// IdempotencyKey is the caller's own key, used to collapse duplicate
	// submissions. Deduplication itself is M3.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// Payload is the JSON body a subscriber receives: the `type` plus `data`
// envelope from https://www.standardwebhooks.com/.
type Payload struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// Payload returns what should be POSTed to a subscriber.
func (r Record) Payload() Payload {
	return Payload{Type: r.Type, Data: r.Data}
}

// EncodeRecord returns relay's Kafka value while preserving Data byte for
// byte. encoding/json compacts RawMessage values during a surrounding marshal,
// which would change whitespace and object-key spelling before the record
// reached Kafka.
func EncodeRecord(r Record) ([]byte, error) {
	if !json.Valid(r.Data) {
		return nil, errors.New("encode record: data is not valid JSON")
	}
	id, err := json.Marshal(r.ID)
	if err != nil {
		return nil, fmt.Errorf("encode record id: %w", err)
	}
	tenantID, err := json.Marshal(r.TenantID)
	if err != nil {
		return nil, fmt.Errorf("encode record tenant: %w", err)
	}
	eventType, err := json.Marshal(r.Type)
	if err != nil {
		return nil, fmt.Errorf("encode record type: %w", err)
	}
	occurredAt, err := json.Marshal(r.OccurredAt)
	if err != nil {
		return nil, fmt.Errorf("encode record occurred_at: %w", err)
	}

	out := make([]byte, 0, len(r.Data)+len(id)+len(tenantID)+len(eventType)+len(occurredAt)+96)
	out = append(out, `{"id":`...)
	out = append(out, id...)
	out = append(out, `,"tenant_id":`...)
	out = append(out, tenantID...)
	out = append(out, `,"type":`...)
	out = append(out, eventType...)
	out = append(out, `,"data":`...)
	out = append(out, r.Data...)
	out = append(out, `,"occurred_at":`...)
	out = append(out, occurredAt...)
	if r.IdempotencyKey != "" {
		key, err := json.Marshal(r.IdempotencyKey)
		if err != nil {
			return nil, fmt.Errorf("encode record idempotency key: %w", err)
		}
		out = append(out, `,"idempotency_key":`...)
		out = append(out, key...)
	}
	out = append(out, '}')
	return out, nil
}

// EncodePayload returns a subscriber request body while preserving Data byte
// for byte.
func EncodePayload(p Payload) ([]byte, error) {
	if !json.Valid(p.Data) {
		return nil, errors.New("encode payload: data is not valid JSON")
	}
	eventType, err := json.Marshal(p.Type)
	if err != nil {
		return nil, fmt.Errorf("encode payload type: %w", err)
	}
	out := make([]byte, 0, len(p.Data)+len(eventType)+18)
	out = append(out, `{"type":`...)
	out = append(out, eventType...)
	out = append(out, `,"data":`...)
	out = append(out, p.Data...)
	out = append(out, '}')
	return out, nil
}

// NewID returns a fresh event id. Random rather than sequential: an id that
// leaks how many events a tenant has sent is an information leak, and relay has
// no need for ordering to be derivable from the id.
func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate event id: %w", err)
	}
	return "evt_" + hex.EncodeToString(b[:]), nil
}

// Validation errors are distinguishable so the HTTP layer can answer 400
// without string-matching.
var (
	ErrNoTenant    = errors.New("tenant_id is required")
	ErrNoType      = errors.New("type is required")
	ErrNoData      = errors.New("data is required")
	ErrDataTooBig  = fmt.Errorf("data exceeds %d bytes", MaxDataBytes)
	ErrDataNotJSON = errors.New("data must be a JSON object or array")
)

// Validate reports whether a record is fit to go on the log. It runs before the
// produce, so a bad event is rejected at the edge rather than becoming a record
// every consumer has to skip forever.
func (r Record) Validate() error {
	if strings.TrimSpace(r.TenantID) == "" {
		return ErrNoTenant
	}
	if !utf8.ValidString(r.TenantID) || !utf8.ValidString(r.Type) {
		return errors.New("tenant_id and type must be valid UTF-8")
	}
	if strings.TrimSpace(r.Type) == "" {
		return ErrNoType
	}
	if len(r.Data) == 0 {
		return ErrNoData
	}
	if len(r.Data) > MaxDataBytes {
		return ErrDataTooBig
	}
	// A bare string or number is legal JSON but not a useful event body, and
	// accepting it here means a subscriber receives `"data": 3`.
	switch r.Data[0] {
	case '{', '[':
	default:
		return ErrDataNotJSON
	}
	if !json.Valid(r.Data) {
		return errors.New("data is not valid JSON")
	}
	return nil
}

// Key is the Kafka partition key. Same tenant, same partition, hence ordering
// per tenant.
func (r Record) Key() []byte { return []byte(r.TenantID) }

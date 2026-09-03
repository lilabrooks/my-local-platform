package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNewIDIsUniqueAndPrefixed(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool, 512)
	for i := 0; i < 512; i++ {
		id, err := NewID()
		if err != nil {
			t.Fatalf("NewID: %v", err)
		}
		if !strings.HasPrefix(id, "evt_") {
			t.Fatalf("id %q lacks the evt_ prefix", id)
		}
		if seen[id] {
			t.Fatalf("NewID returned %q twice", id)
		}
		seen[id] = true
	}
}

func validRecord() Record {
	return Record{
		ID:         "evt_test",
		TenantID:   "acme",
		Type:       "invoice.paid",
		Data:       json.RawMessage(`{"amount":100}`),
		OccurredAt: time.Now().UTC(),
	}
}

func TestValidateAccepts(t *testing.T) {
	t.Parallel()

	if err := validRecord().Validate(); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}

	arr := validRecord()
	arr.Data = json.RawMessage(`[1,2,3]`)
	if err := arr.Validate(); err != nil {
		t.Errorf("array data rejected: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		mutate func(*Record)
		want   error
	}{
		"no tenant":      {func(r *Record) { r.TenantID = "" }, ErrNoTenant},
		"blank tenant":   {func(r *Record) { r.TenantID = "   " }, ErrNoTenant},
		"no type":        {func(r *Record) { r.Type = "" }, ErrNoType},
		"blank type":     {func(r *Record) { r.Type = "  " }, ErrNoType},
		"no data":        {func(r *Record) { r.Data = nil }, ErrNoData},
		"empty data":     {func(r *Record) { r.Data = json.RawMessage(``) }, ErrNoData},
		"scalar string":  {func(r *Record) { r.Data = json.RawMessage(`"hello"`) }, ErrDataNotJSON},
		"scalar number":  {func(r *Record) { r.Data = json.RawMessage(`3`) }, ErrDataNotJSON},
		"data too large": {func(r *Record) { r.Data = json.RawMessage(`{"a":"` + strings.Repeat("x", MaxDataBytes) + `"}`) }, ErrDataTooBig},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := validRecord()
			tc.mutate(&r)
			err := r.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want %v", tc.want)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("Validate() = %v, want %v", err, tc.want)
			}
		})
	}
}

// Malformed JSON that starts with { must still be caught, or it becomes a
// record every consumer has to skip forever.
func TestValidateRejectsBrokenJSON(t *testing.T) {
	t.Parallel()

	r := validRecord()
	r.Data = json.RawMessage(`{"unterminated": `)
	if err := r.Validate(); err == nil {
		t.Fatal("truncated JSON object accepted, want an error")
	}
}

// The subscriber body is exactly the specification's envelope: no tenant, no
// internal id, no timestamp. Those live on the record and in headers.
func TestPayloadIsOnlyTypeAndData(t *testing.T) {
	t.Parallel()

	r := validRecord()
	r.IdempotencyKey = "caller-key"
	r.Data = json.RawMessage("{\n  \"amount\": 100\n}")

	encoded, err := EncodePayload(r.Payload())
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("payload has %d fields (%s), want exactly type and data", len(got), encoded)
	}
	for _, key := range []string{"type", "data"} {
		if _, ok := got[key]; !ok {
			t.Errorf("payload is missing %q: %s", key, encoded)
		}
	}
	for _, leaked := range []string{"acme", "evt_test", "caller-key"} {
		if strings.Contains(string(encoded), leaked) {
			t.Errorf("payload leaked internal field %q: %s", leaked, encoded)
		}
	}
	if !bytes.Contains(encoded, append([]byte(`"data":`), r.Data...)) {
		t.Errorf("payload changed data bytes: %q", encoded)
	}
}

// The record must survive a round trip through the log unchanged, since the
// delivery consumer reads back exactly what ingest wrote.
func TestRecordRoundTrip(t *testing.T) {
	t.Parallel()

	want := validRecord()
	want.IdempotencyKey = "abc123"
	want.Data = json.RawMessage("{\n  \"b\": 2, \"a\": 1\n}")

	encoded, err := EncodeRecord(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Record
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.ID != want.ID || got.TenantID != want.TenantID || got.Type != want.Type {
		t.Errorf("round trip changed identity: %+v", got)
	}
	if string(got.Data) != string(want.Data) {
		t.Errorf("data = %s, want %s", got.Data, want.Data)
	}
	if !bytes.Contains(encoded, append([]byte(`"data":`), want.Data...)) {
		t.Errorf("record changed data bytes: %q", encoded)
	}
	if !got.OccurredAt.Equal(want.OccurredAt) {
		t.Errorf("occurred_at = %s, want %s", got.OccurredAt, want.OccurredAt)
	}
}

func TestKeyIsTenant(t *testing.T) {
	t.Parallel()

	r := validRecord()
	if got, want := string(r.Key()), "acme"; got != want {
		t.Errorf("Key() = %q, want %q -- the partition key is what buys per-tenant ordering", got, want)
	}
}

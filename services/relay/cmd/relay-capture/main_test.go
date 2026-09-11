package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestCaptureInputRestrictsTopicsAndCompleteOffsets(t *testing.T) {
	starts := map[int]int64{}
	for p := 0; p < 12; p++ {
		starts[p] = int64(p)
	}
	if err := validateInput("read", deliveries, starts); err != nil {
		t.Fatal(err)
	}
	if err := validateInput("read", dlq, map[int]int64{0: 0}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		op, topic string
		starts    map[int]int64
	}{
		{"read", deliveries, map[int]int64{0: 0}},
		{"read", dlq, map[int]int64{0: -1}},
		{"read", dlq, map[int]int64{1: 0}},
		{"snapshot", "unapproved-topic", nil},
		{"delete", deliveries, nil},
	} {
		if validateInput(tc.op, tc.topic, tc.starts) == nil {
			t.Fatalf("accepted %#v", tc)
		}
	}
}

func TestCapturePreservesKafkaCoordinatesAndBinaryPayload(t *testing.T) {
	m := kafka.Message{Topic: deliveries, Partition: 3, Offset: 42, Time: time.Unix(10, 123000000).UTC(), Key: []byte{0, 255}, Value: []byte("{poison")}
	value := captured(m)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var restored record
	if err = json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Topic != m.Topic || restored.Partition != 3 || restored.Offset != 42 || !restored.Timestamp.Equal(m.Time) || string(restored.Key) != string(m.Key) || string(restored.Value) != string(m.Value) {
		t.Fatal("record changed during export")
	}
}

func TestReadRefusesUnboundedIntervalsBeforeConnecting(t *testing.T) {
	for _, end := range []int64{1, 2003} {
		if _, err := readSnapshot(context.Background(), nil, dlq, map[int]int64{0: 2}, map[int]int64{0: end}); err == nil {
			t.Fatal("invalid interval accepted")
		}
	}
}

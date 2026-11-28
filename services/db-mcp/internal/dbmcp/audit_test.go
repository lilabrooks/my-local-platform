package dbmcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"
)

type auditWriterFunc func([]byte) (int, error)

func (f auditWriterFunc) Write(b []byte) (int, error) { return f(b) }

func waitAuditSlot(t *testing.T, sink *AuditSink) *AuditSlot {
	t.Helper()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if slot := sink.Reserve(); slot != nil {
			return slot
		}
		select {
		case <-timeout.C:
			t.Fatal("audit admission did not reopen")
		case <-tick.C:
		}
	}
}

func TestAuditCapacityReservedBeforeWork(t *testing.T) {
	sink := NewAuditSink(io.Discard)
	var slots []*AuditSlot
	for range auditCapacity {
		slot := sink.Reserve()
		if slot == nil {
			t.Fatal("capacity exhausted early")
		}
		slots = append(slots, slot)
	}
	if sink.Reserve() != nil {
		t.Fatal("admission exceeded bounded capacity")
	}
	for _, slot := range slots {
		slot.Release()
	}
	if !sink.Reserve().Record(auditRecord{Outcome: "ok"}, auditRecord{Outcome: "unavailable"}) {
		t.Fatal("unused reservations prevented recovery")
	}
	if NewAuditSink(nil).Reserve() != nil {
		t.Fatal("missing sink accepted work")
	}
}

func TestAuditStalledWriterPreservesTerminalOutcome(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})
	sink := NewAuditSink(writer)
	attempt := auditRecord{ID: "one", Caller: "local-test", Operation: "whoami", Outcome: "ok", Rows: new(int)}
	terminal := attempt
	terminal.Outcome, terminal.AuditState, terminal.Rows = "unavailable", "response_withheld", nil
	started := time.Now()
	if sink.Reserve().Record(attempt, terminal) {
		t.Fatal("a stalled write released the result")
	}
	elapsed := time.Since(started)
	if elapsed < AuditWriteTimeout || elapsed > 2*time.Second {
		t.Fatalf("audit acknowledgement took %s", elapsed)
	}
	if sink.Reserve() != nil {
		t.Fatal("new work admitted during a blocked write")
	}
	stopRead := time.AfterFunc(2*time.Second, func() { _ = reader.CloseWithError(io.ErrNoProgress) })
	defer stopRead.Stop()
	lines := bufio.NewReader(reader)
	for _, want := range []auditRecord{attempt, terminal} {
		line, err := lines.ReadBytes('\n')
		var got auditRecord
		if err != nil || json.Unmarshal(line, &got) != nil || got.ID != want.ID || got.Outcome != want.Outcome ||
			got.Caller != want.Caller || got.Operation != want.Operation || got.AuditState != want.AuditState || (got.Rows == nil) != (want.Rows == nil) {
			t.Fatalf("recovered record %s, error %v", line, err)
		}
	}
	waitAuditSlot(t, sink).Release()
	t.Logf("stalled write withheld the result in %s; both records correlated after draining", elapsed.Round(time.Millisecond))
}

func TestAuditFailedAndShortWritesRecover(t *testing.T) {
	for _, tc := range []struct {
		name    string
		partial bool
	}{{"write error", false}, {"partial attempted and terminal records", true}} {
		t.Run(tc.name, func(t *testing.T) {
			var log bytes.Buffer
			writes := 0
			sink := NewAuditSink(auditWriterFunc(func(line []byte) (int, error) {
				writes++
				if writes == 1 {
					if tc.partial {
						return log.Write(line[:7]) // Deliberately short, with a nil error.
					}
					return 0, io.ErrClosedPipe
				}
				if tc.partial && writes == 3 {
					n, _ := log.Write(line[:5])
					return n, io.ErrClosedPipe
				}
				return log.Write(line)
			}))
			attempt := auditRecord{ID: "one", Outcome: "ok"}
			terminal := auditRecord{ID: "one", Outcome: "unavailable", AuditState: "response_withheld"}
			if sink.Reserve().Record(attempt, terminal) || sink.Reserve() != nil {
				t.Fatal("failed audit released data or admitted the recovery probe")
			}
			slot := waitAuditSlot(t, sink)
			next := auditRecord{ID: "two", Outcome: "ok"}
			if !slot.Record(next, auditRecord{ID: "two", Outcome: "unavailable"}) {
				t.Fatal("healthy sink failed to acknowledge the next request")
			}
			expect := []auditRecord{terminal, next}
			wantWrites := 3
			if tc.partial {
				expect = []auditRecord{attempt, terminal, next}
				wantWrites = 5
			}
			decoder := json.NewDecoder(&log)
			for _, want := range expect {
				var got auditRecord
				if err := decoder.Decode(&got); err != nil || got != want {
					t.Fatalf("recovered record=%+v, want %+v, error %v", got, want, err)
				}
			}
			if err := decoder.Decode(new(auditRecord)); err != io.EOF || writes != wantWrites {
				t.Fatalf("wrong end of recovered stream: %v with %d writes", err, writes)
			}
		})
	}
}

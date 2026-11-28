//go:build awspilot

package pilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// FilterLogEvents returns JSON strings from events[].message. Check every
// observed record, including repeats, before using it as evidence of delivery.
func matchAuditMessages(data []byte, expected map[string]auditEvent) (int, error) {
	var messages []string
	if err := json.Unmarshal(data, &messages); err != nil {
		return 0, errors.New("invalid CloudWatch message list")
	}
	seen := make(map[string]bool)
	for _, message := range messages {
		var got auditEvent
		if err := json.Unmarshal([]byte(message), &got); err != nil {
			return 0, errors.New("invalid CloudWatch audit record")
		}
		want, ok := expected[got.RequestID]
		if !ok || got.Caller != want.Caller || got.Operation != want.Operation || got.Outcome != want.Outcome {
			return 0, errors.New("unexpected request, caller, operation or outcome in CloudWatch")
		}
		if got.AuditState != "" {
			return 0, errors.New("the pilot response was withheld by its audit destination")
		}
		seen[got.RequestID] = true
	}
	return len(seen), nil
}

func waitForPilotAudit(ctx context.Context, calls []auditEvent, hourly bool,
	read func(context.Context, string, string) ([]byte, error), interval time.Duration) error {
	router := make(map[string]auditEvent)
	executor := make(map[string]auditEvent)
	var terms []string
	for _, call := range calls {
		if !requestIDPattern.MatchString(call.RequestID) {
			return errors.New("invalid audit request ID")
		}
		router[call.RequestID] = call
		terms = append(terms, fmt.Sprintf("($.request_id = %q)", call.RequestID))
		if call.Operation == "run_sql" && call.Outcome == "ok" {
			executor[call.RequestID] = call
		}
	}
	if len(calls) != 6 || len(router) != len(calls) || hourly && len(executor) != 1 || !hourly && len(executor) != 0 {
		return errors.New("incomplete pilot audit expectations")
	}
	pattern := "{ " + strings.Join(terms, " || ") + " }"
	groups := []string{"router"}
	if hourly {
		groups = append(groups, "executor")
	}
	previousComplete := false
	for {
		pending := ""
		for _, group := range groups {
			want := router
			if group == "executor" {
				want = executor
			}
			// The same filter includes all six IDs on both groups. An observer
			// record on the executor is an error even if the reader arrived.
			data, err := read(ctx, "/aws/lambda/mlp-db-mcp-"+group, pattern)
			if err != nil {
				return fmt.Errorf("read %s audit group: %w", group, err)
			}
			matched, err := matchAuditMessages(data, want)
			if err != nil {
				return fmt.Errorf("%s audit: %w", group, err)
			}
			if matched != len(want) {
				pending = fmt.Sprintf("%s delivered %d of %d records", group, matched, len(want))
			}
		}
		if pending == "" && (!hourly || previousComplete) {
			return nil
		}
		previousComplete = pending == ""
		if pending == "" {
			pending = "awaiting a second hourly observation after delivery"
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("CloudWatch audit incomplete (%s): %w", pending, ctx.Err())
		case <-time.After(interval):
		}
	}
}

func TestCloudWatchPilotAudit(t *testing.T) {
	var calls []auditEvent
	for _, caller := range []string{"reader", "observer"} {
		for _, op := range []string{"whoami", "list_targets", "run_sql"} {
			outcome := "ok"
			if caller == "observer" && op == "run_sql" {
				outcome = "denied"
			}
			calls = append(calls, auditEvent{RequestID: fmt.Sprintf("%032x", len(calls)+1), Caller: caller, Operation: op, Outcome: outcome})
		}
	}
	encode := func(records []auditEvent) []byte {
		var messages []string
		for _, record := range records {
			message, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			messages = append(messages, string(message))
		}
		result, err := json.Marshal(messages)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	for _, tc := range []struct {
		name   string
		hourly bool
	}{
		{"identity", false},
		{"hourly", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := append([]auditEvent(nil), calls...)
			if !tc.hourly {
				want[2].Outcome = "unavailable"
			}
			var routers, executors int
			read := func(_ context.Context, group, pattern string) ([]byte, error) {
				for _, event := range want {
					if !strings.Contains(pattern, fmt.Sprintf("($.request_id = %q)", event.RequestID)) {
						t.Fatal("filter omitted an expected request")
					}
				}
				switch group {
				case "/aws/lambda/mlp-db-mcp-router":
					routers++
					if routers == 1 {
						return encode(want[:1]), nil
					}
					return encode(want), nil
				case "/aws/lambda/mlp-db-mcp-executor":
					if !tc.hourly {
						t.Fatal("identity stage queried the executor")
					}
					executors++
					return encode(want[2:3]), nil
				default:
					t.Fatal("unexpected CloudWatch group")
					return nil, nil
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := waitForPilotAudit(ctx, want, tc.hourly, read, time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if routers < 2 || tc.hourly && executors < 3 {
				t.Fatal("finished before delivery and the hourly follow-up observation")
			}
		})
	}
	t.Run("missing executor delivery", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		read := func(_ context.Context, group, _ string) ([]byte, error) {
			if strings.HasSuffix(group, "executor") {
				cancel()
				return []byte("[]"), nil
			}
			return encode(calls), nil
		}
		if err := waitForPilotAudit(ctx, calls, true, read, time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("missing delivery returned %v", err)
		}
	})
	t.Run("audit recovery cannot satisfy identity-stage unavailability", func(t *testing.T) {
		reader := calls[2]
		reader.Outcome = "unavailable"
		correction := reader
		correction.AuditState = "response_withheld"
		expected := map[string]auditEvent{reader.RequestID: reader}
		for _, messages := range [][]auditEvent{{correction}, {reader, correction}} {
			if _, err := matchAuditMessages(encode(messages), expected); err == nil {
				t.Fatal("accepted a withheld response as the identity-stage reader result")
			}
		}
	})
	t.Run("observer appears after reader delivery", func(t *testing.T) {
		observations := 0
		read := func(_ context.Context, group, _ string) ([]byte, error) {
			if strings.HasSuffix(group, "executor") {
				observations++
				if observations > 1 {
					return encode([]auditEvent{calls[2], calls[5]}), nil
				}
				return encode(calls[2:3]), nil
			}
			return encode(calls), nil
		}
		if err := waitForPilotAudit(t.Context(), calls, true, read, time.Millisecond); err == nil {
			t.Fatal("accepted observer executor record")
		}
	})
	wrong := calls[2]
	wrong.Outcome = "unavailable"
	if _, err := matchAuditMessages(encode([]auditEvent{wrong}), map[string]auditEvent{calls[2].RequestID: calls[2]}); err == nil {
		t.Fatal("accepted the wrong audit outcome")
	}
}

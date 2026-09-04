package checks

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// A check's detail and its error are built from whatever the check saw:
// subscriber URLs, HTTP response bodies, decoded records. A span leaves the
// host; stdout does not. This asserts the boundary directly, on the recorded
// spans, rather than trusting the wrapper to stay small.
//
// The strings below are the real shapes. `dead-lettered http://sink:8081/...`
// is quoted from a live Tempo trace on 2026-09-03, which is how the leak was
// found: it reached a `check.detail` attribute that this wrapper used to set.
func TestInstrumentKeepsCheckStringsOffSpans(t *testing.T) {
	t.Parallel()

	const (
		successDetail = "evt_abc delivered to http://sink:8081/hooks/ok, " +
			"dead-lettered http://sink:8081/hooks/flaky after 3 attempts"
		failureDetail = "partial work before the failure"
		responseBody  = `{"error":"tenant acme is not provisioned","token":"s3cret"}`
	)
	failure := fmt.Errorf("ingest returned 500, want 202: %s", responseBody)

	list := []Check{
		{Name: "passing", Run: func(context.Context) (string, error) {
			return successDetail, nil
		}},
		{Name: "failing", Run: func(context.Context) (string, error) {
			return failureDetail, failure
		}},
		{Name: "timing-out", Run: func(context.Context) (string, error) {
			return "", fmt.Errorf("await delivery: %w", context.DeadlineExceeded)
		}},
	}

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	results := Run(context.Background(), time.Second, Instrument(provider.Tracer("smoke"), list))

	// The details still reach the caller, and so stdout via Report. Only the
	// spans are stripped.
	if len(results) != 3 || results[0].Detail != successDetail || results[1].Detail != failureDetail {
		t.Fatalf("Instrument changed what the runner reports: %+v", results)
	}

	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("recorded %d spans, want 3", len(spans))
	}
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range spans {
		byName[span.Name()] = span
	}
	for _, want := range []string{"check.passing", "check.failing", "check.timing-out"} {
		if byName[want] == nil {
			t.Fatalf("no %s span; got %v", want, byName)
		}
	}

	if code := byName["check.passing"].Status().Code; code == codes.Error {
		t.Errorf("passing check span status = Error, want Unset or Ok")
	}
	for name, wantType := range map[string]string{
		"check.failing":    "error",
		"check.timing-out": "timeout",
	} {
		span := byName[name]
		if span.Status().Code != codes.Error || span.Status().Description != "check failed" {
			t.Errorf("%s status = %v %q, want Error \"check failed\"",
				name, span.Status().Code, span.Status().Description)
		}
		if len(span.Events()) != 0 {
			t.Errorf("%s carries %d events, want none: RecordError's exception.message "+
				"is the error string verbatim", name, len(span.Events()))
		}
		var errorType string
		for _, attr := range span.Attributes() {
			if attr.Key == "error.type" {
				errorType = attr.Value.String()
			}
		}
		if errorType != wantType {
			t.Errorf("%s error.type = %q, want %q", name, errorType, wantType)
		}
	}

	// Nothing a check produced may appear anywhere in any span.
	forbidden := []string{
		"sink:8081", "hooks/ok", "hooks/flaky", "evt_abc",
		responseBody, "s3cret", "acme", failureDetail, "await delivery",
	}
	for _, span := range spans {
		exported := fmt.Sprintf("%v %v %v", span.Name(), span.Status(), span.Attributes())
		for _, e := range span.Events() {
			exported += fmt.Sprintf(" %v %v", e.Name, e.Attributes)
		}
		for _, secret := range forbidden {
			if strings.Contains(exported, secret) {
				t.Errorf("%s exports %q, which came from the check: %s", span.Name(), secret, exported)
			}
		}
	}
}

// A nil tracer means telemetry failed to start. The checks must still run.
func TestInstrumentWithoutATracerLeavesChecksAlone(t *testing.T) {
	t.Parallel()

	list := []Check{{Name: "x", Run: func(context.Context) (string, error) { return "ran", nil }}}
	results := Run(context.Background(), time.Second, Instrument(nil, list))
	if len(results) != 1 || results[0].Detail != "ran" || !results[0].OK() {
		t.Fatalf("results = %+v, want one passing check", results)
	}
}

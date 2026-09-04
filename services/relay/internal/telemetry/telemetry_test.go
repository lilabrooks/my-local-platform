package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaHeaderCarrierInjectsAndExtractsTraceContext(t *testing.T) {
	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
	want := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), want)

	headers := []kafka.Header{{Key: "unrelated", Value: []byte("kept")}}
	carrier := NewKafkaHeaderCarrier(&headers)
	propagator := propagation.TraceContext{}
	propagator.Inject(ctx, carrier)

	got := trace.SpanContextFromContext(propagator.Extract(context.Background(), carrier))
	if got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() || !got.IsSampled() {
		t.Fatalf("extracted span context = %v, want %v", got, want)
	}
	if carrier.Get("unrelated") != "kept" {
		t.Fatal("injecting trace context replaced an unrelated Kafka header")
	}
}

func TestKafkaHeaderCarrierRejectsMalformedTraceParent(t *testing.T) {
	headers := []kafka.Header{{Key: "traceparent", Value: []byte("not-a-traceparent")}}
	ctx := propagation.TraceContext{}.Extract(context.Background(), NewKafkaHeaderCarrier(&headers))
	if got := trace.SpanContextFromContext(ctx); got.IsValid() {
		t.Fatalf("malformed traceparent produced valid context %v", got)
	}
}

// A duplicate is not hypothetical bookkeeping: Get reads the LAST match, so a
// Set that fixed only the first would leave the stale duplicate winning at the
// next hop, and the trace would fork with nothing reporting an error.
func TestKafkaHeaderCarrierSetRemovesDuplicateHeaders(t *testing.T) {
	headers := []kafka.Header{
		{Key: "traceparent", Value: []byte("00-11111111111111111111111111111111-1111111111111111-01")},
		{Key: "unrelated", Value: []byte("kept")},
		{Key: "TraceParent", Value: []byte("00-22222222222222222222222222222222-2222222222222222-01")},
	}
	carrier := NewKafkaHeaderCarrier(&headers)

	want := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
		SpanID:     trace.SpanID{3, 3, 3, 3, 3, 3, 3, 3},
		TraceFlags: trace.FlagsSampled,
	})
	propagator := propagation.TraceContext{}
	propagator.Inject(trace.ContextWithSpanContext(context.Background(), want), carrier)

	got := trace.SpanContextFromContext(propagator.Extract(context.Background(), carrier))
	if got.TraceID() != want.TraceID() {
		t.Errorf("extracted trace id = %s, want the injected %s", got.TraceID(), want.TraceID())
	}
	traceparents := 0
	for _, header := range headers {
		if strings.EqualFold(header.Key, "traceparent") {
			traceparents++
		}
	}
	if traceparents != 1 {
		t.Errorf("traceparent appears %d times, want 1: %v", traceparents, headers)
	}
	if carrier.Get("unrelated") != "kept" {
		t.Errorf("unrelated header = %q, want kept", carrier.Get("unrelated"))
	}
}

func TestKafkaHeaderCarrierToleratesNilHeaders(t *testing.T) {
	carrier := KafkaHeaderCarrier{}
	carrier.Set("traceparent", "x") // must not panic
	if got := carrier.Get("traceparent"); got != "" {
		t.Errorf("Get = %q, want empty", got)
	}
	if got := carrier.Keys(); got != nil {
		t.Errorf("Keys = %v, want nil", got)
	}
}

// The point of RecordError is what it does NOT put on the span. Both errors
// here carry data that must not leave the service: the subscriber URL with its
// query string, and a caller-chosen idempotency key.
func TestRecordErrorKeepsErrorMessagesOffTheSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	transportErr := &url.Error{
		Op:  "Post",
		URL: "http://subscriber.internal/hooks/abc?token=s3cret",
		Err: syscall.ECONNREFUSED,
	}
	conflictErr := fmt.Errorf("idempotency conflict: tenant %q key %q", "acme", "customer-42-order-99")

	for _, tc := range []struct {
		name        string
		err         error
		description string
		wantType    string
		forbidden   []string
	}{
		{"transport", transportErr, "webhook request failed", "connection_refused",
			[]string{"subscriber.internal", "s3cret", "/hooks/abc"}},
		{"conflict", conflictErr, "idempotency conflict", "error",
			[]string{"customer-42-order-99", "acme"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, span := provider.Tracer("test").Start(context.Background(), tc.name)
			RecordError(span, tc.err, tc.description)
			span.End()

			var got sdktrace.ReadOnlySpan
			for _, s := range recorder.Ended() {
				if s.Name() == tc.name {
					got = s
				}
			}
			if got == nil {
				t.Fatalf("span %q was not recorded", tc.name)
			}
			if got.Status().Code != codes.Error || got.Status().Description != tc.description {
				t.Errorf("status = %v %q, want Error %q",
					got.Status().Code, got.Status().Description, tc.description)
			}
			if len(got.Events()) != 0 {
				t.Errorf("span carries %d events, want none: RecordError must not add an "+
					"exception event, whose exception.message is the leak", len(got.Events()))
			}
			var errorType string
			for _, attr := range got.Attributes() {
				if attr.Key == "error.type" {
					errorType = attr.Value.AsString()
				}
			}
			if errorType != tc.wantType {
				t.Errorf("error.type = %q, want %q", errorType, tc.wantType)
			}
			// Everything the span would export, in one string.
			exported := fmt.Sprintf("%v %v %v", got.Name(), got.Status(), got.Attributes())
			for _, secret := range tc.forbidden {
				if strings.Contains(exported, secret) {
					t.Errorf("span exports %q, which came from the error message: %s", secret, exported)
				}
			}
		})
	}
}

func TestErrorTypeClassifiesWithoutCopyingTheMessage(t *testing.T) {
	timeout := &url.Error{Op: "Post", URL: "http://x/y", Err: context.DeadlineExceeded}
	for _, tc := range []struct {
		err  error
		want string
	}{
		{nil, ""},
		{context.DeadlineExceeded, "timeout"},
		{context.Canceled, "canceled"},
		{timeout, "timeout"},
		{&url.Error{Op: "Post", URL: "http://x/y", Err: syscall.ECONNREFUSED}, "connection_refused"},
		{&url.Error{Op: "Post", URL: "http://x/y", Err: &net.DNSError{Err: "no such host"}}, "dns"},
		{&url.Error{Op: "Post", URL: "http://x/y", Err: errors.New("boom")}, "http_request"},
		{errors.New("boom"), "error"},
	} {
		if got := ErrorType(tc.err); got != tc.want {
			t.Errorf("ErrorType(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

// Issue #86 asks for traceparent and tracestate. Baggage would additionally
// make relay forward arbitrary caller-supplied pairs from an ingest request,
// through Kafka, into every signed subscriber request -- a trust boundary
// nobody asked relay to open. Asserting on the installed propagator rather than
// on one constructed in a test is the point: this is what Init actually sets.
func TestInitInstallsTraceContextWithoutBaggage(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })

	// A dead endpoint: the OTLP gRPC exporter connects lazily, so Init succeeds
	// and relay starts with no collector -- which is also the CI topology.
	shutdown, err := Init(context.Background(), "relay-test", "test", "http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("Init with an unreachable collector = %v, want nil: relay must still start", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = shutdown(ctx)
	})

	fields := otel.GetTextMapPropagator().Fields()
	want := map[string]bool{"traceparent": true, "tracestate": true}
	for _, field := range fields {
		if !want[field] {
			t.Errorf("propagator injects %q; relay propagates trace context only", field)
		}
		delete(want, field)
	}
	for field := range want {
		t.Errorf("propagator does not inject %q", field)
	}
}

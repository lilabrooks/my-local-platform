// Package telemetry owns relay's vendor-neutral OpenTelemetry setup and
// propagation adapters.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// Init sends relay spans over OTLP. The collector chooses their destination.
func Init(ctx context.Context, serviceName, environment, endpoint string) (func(context.Context) error, error) {
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
		attribute.String("deployment.environment", environment),
	))
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// TraceContext only, deliberately: no Baggage.
	//
	// Baggage would make relay a forwarder for arbitrary caller-supplied
	// key-value pairs. An ingest client's `baggage` header would cross into
	// Kafka headers, out of relay-deliver, and into every signed subscriber
	// request -- a trust boundary nobody asked relay to open. Issue #86 asks
	// for `traceparent` and `tracestate`, which is exactly what this carries.
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}

// RecordError puts err on span as a classification rather than as its message,
// and marks the span failed with a fixed description.
//
// It exists because span.RecordError would export err.Error() verbatim as
// exception.message, and relay's error strings carry data that must not leave
// the service. Two concrete cases, both live:
//
//   - net/http returns *url.Error from Client.Do, whose message embeds the
//     request URL. Only the password is redacted, so a subscriber's host, path
//     and query string -- which may itself carry a token -- would ship to the
//     trace backend, and onward to any vendor exporter the collector defines.
//   - history.ErrIdempotencyConflict names the caller's idempotency key, which
//     is caller-chosen text and can be an order id or a customer reference.
//
// The full error still reaches the service log and, for a delivery attempt, the
// attempt-history row. Neither leaves the host. A span does.
func RecordError(span trace.Span, err error, description string) {
	if span == nil || err == nil {
		return
	}
	span.SetAttributes(attribute.String("error.type", ErrorType(err)))
	span.SetStatus(codes.Error, description)
}

// ErrorType classifies err into a token that carries no data from the error's
// message. Unrecognised errors report "error" rather than anything derived from
// their contents, because the point is that an unrecognised error is exactly
// the one whose message has not been reviewed.
func ErrorType(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	}

	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "dns"
	}
	// After DNS and the syscall cases, so the specific classification wins over
	// the generic timeout one.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return "http_request"
	}
	return "error"
}

// KafkaHeaderCarrier adapts kafka-go headers to W3C text-map propagation.
// Set replaces an existing header because traceparent and tracestate are
// single-valued fields.
type KafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

// NewKafkaHeaderCarrier returns a carrier backed by headers.
func NewKafkaHeaderCarrier(headers *[]kafka.Header) KafkaHeaderCarrier {
	return KafkaHeaderCarrier{headers: headers}
}

// Get returns the last value for key, matching HTTP's overwrite semantics.
func (c KafkaHeaderCarrier) Get(key string) string {
	if c.headers == nil {
		return ""
	}
	for i := len(*c.headers) - 1; i >= 0; i-- {
		if strings.EqualFold((*c.headers)[i].Key, key) {
			return string((*c.headers)[i].Value)
		}
	}
	return ""
}

// Set replaces every occurrence of key with one header, or appends it when
// absent.
//
// Every occurrence, not the first: Get reads the LAST match, so replacing only
// the first would leave a later duplicate shadowing the value just injected --
// silently forking the trace at the next hop. Kafka headers, unlike HTTP ones,
// are a list that permits repeats, so a duplicate is a shape this carrier has
// to survive rather than one it can assume away.
func (c KafkaHeaderCarrier) Set(key, value string) {
	if c.headers == nil {
		return
	}
	headers := *c.headers
	replaced := false
	kept := headers[:0]
	for _, header := range headers {
		if !strings.EqualFold(header.Key, key) {
			kept = append(kept, header)
			continue
		}
		if replaced {
			continue // drop the duplicate
		}
		kept = append(kept, kafka.Header{Key: key, Value: []byte(value)})
		replaced = true
	}
	if !replaced {
		kept = append(kept, kafka.Header{Key: key, Value: []byte(value)})
	}
	*c.headers = kept
}

// Keys reports the available header names.
func (c KafkaHeaderCarrier) Keys() []string {
	if c.headers == nil {
		return nil
	}
	keys := make([]string, 0, len(*c.headers))
	for _, header := range *c.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

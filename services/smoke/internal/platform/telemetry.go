package platform

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// InitTracing wires OTLP tracing to the collector. Nothing here names a
// vendor: the collector decides whether spans go to Tempo, Datadog, both, or
// nowhere. That indirection is the reason this is worth doing at all.
//
// The returned shutdown func flushes pending spans; call it before exit or the
// last spans of a short-lived process are lost.
func InitTracing(ctx context.Context, cfg Config) (trace.Tracer, func(context.Context) error, error) {
	// The gRPC exporter wants host:port, not a URL.
	endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.OTLPEndpoint, "http://"), "https://")

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		attribute.String("deployment.environment", "local"),
	))
	if err != nil {
		return nil, nil, fmt.Errorf("resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	return tp.Tracer(cfg.ServiceName), tp.Shutdown, nil
}

// ErrorType classifies err into a token that carries nothing from the error's
// message. Unrecognised errors report "error" rather than anything derived from
// their contents, because an unrecognised error is exactly the one whose text
// has not been reviewed.
//
// Smoke check errors are built with fmt.Errorf from whatever the check saw:
// subscriber URLs, HTTP response bodies, decoded records. None of that belongs
// on a span. The full text goes to stdout, which is where a person reads it.
//
// This mirrors relay's telemetry.ErrorType. The duplication is deliberate --
// they are separate Go modules, and relay's lives in an internal package.
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

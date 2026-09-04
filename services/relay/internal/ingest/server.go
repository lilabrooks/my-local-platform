// Package ingest accepts events over HTTP and puts them on the log.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/lilabrooks/my-local-platform/relay/config"
	"github.com/lilabrooks/my-local-platform/relay/internal/event"
	"github.com/lilabrooks/my-local-platform/relay/internal/history"
	"github.com/lilabrooks/my-local-platform/relay/internal/metrics"
	"github.com/lilabrooks/my-local-platform/relay/internal/telemetry"
)

const (
	// MaxBodyBytes bounds a request before it is parsed. Slightly above
	// event.MaxDataBytes so an oversized payload is reported as "data too large"
	// rather than as a truncated-JSON parse error, which sends the caller looking
	// in the wrong place.
	MaxBodyBytes = event.MaxDataBytes + 8*1024

	// acceptanceTimeout lets a valid request finish its Postgres and Kafka
	// acceptance after the client disconnects. kafka.Writer bounds its broker
	// write at 10 seconds; this leaves time to record the acknowledgement and
	// release the row lock.
	//
	// Defined in config because relay-ingest's graceful shutdown has to wait
	// at least this long before closing the pool and the writer -- otherwise
	// shutdown destroys the very work this timeout exists to complete.
	acceptanceTimeout = config.IngestAcceptanceTimeout
)

// Producer is the part of kafka.Writer this package needs. An interface so the
// handler is testable without a broker.
type Producer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// EventStore persists accepted events and answers their delivery history.
type EventStore interface {
	AcceptEvent(context.Context, event.Record, history.Publisher) (history.Acceptance, error)
	Attempts(context.Context, string) ([]history.Attempt, error)
}

// Server accepts events and produces them.
type Server struct {
	producer Producer
	events   EventStore
	topic    string
	log      *slog.Logger
	ready    atomic.Bool
	accepted atomic.Int64
}

// New returns a Server that is not yet ready; call MarkReady once the broker
// connection has been established.
func New(p Producer, events EventStore, topic string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{producer: p, events: events, topic: topic, log: log}
}

// MarkReady flips readiness. Kubernetes pulls the pod from its Service when
// this goes false, which is how a shutdown drains.
func (s *Server) MarkReady(ready bool) { s.ready.Store(ready) }

// Routes returns the handler.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.postEvent)
	mux.HandleFunc("GET /v1/events/{id}/attempts", s.getAttempts)
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"status":   "ready",
			"accepted": fmt.Sprint(s.accepted.Load()),
		})
	})
	return mux
}

type postEventRequest struct {
	TenantID       string          `json:"tenant_id"`
	Type           string          `json:"type"`
	Data           json.RawMessage `json:"data"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type postEventResponse struct {
	ID string `json:"id"`
}

func (s *Server) postEvent(w http.ResponseWriter, r *http.Request) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := otel.Tracer("relay/ingest").Start(ctx, "relay.ingest",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path),
		),
	)
	defer span.End()

	body := http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields() // a misspelled field is a caller bug, not a default

	var req postEventRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			metrics.IngestEvents.WithLabelValues("too_large").Inc()
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds %d bytes", MaxBodyBytes)
			return
		}
		metrics.IngestEvents.WithLabelValues("malformed").Inc()
		writeError(w, http.StatusBadRequest, "malformed JSON: %v", err)
		return
	}

	// A request body is ONE JSON text, not a stream of them. Decode stops at
	// the end of the first value and reports no error, so without this a body
	// like `{"tenant_id":"acme",...}{"tenant_id":"evil"}` is accepted, the
	// first object is published, and the second is silently discarded --
	// relay's idea of the request and the caller's differ, with a 202 saying
	// they agree. Two concatenated values are not one JSON text under RFC 8259.
	//
	// More() skips trailing whitespace, so a body ending in a newline is still
	// accepted; TestTrailingWhitespaceIsAccepted holds that line.
	if dec.More() {
		metrics.IngestEvents.WithLabelValues("malformed").Inc()
		writeError(w, http.StatusBadRequest,
			"malformed JSON: unexpected content after the first JSON value")
		return
	}

	id, err := event.NewID()
	if err != nil {
		// Losing the random source is not the caller's fault.
		s.log.Error("generate event id", "error", err)
		metrics.IngestEvents.WithLabelValues("internal").Inc()
		writeError(w, http.StatusInternalServerError, "could not generate an event id")
		return
	}

	idempotencyKey := req.IdempotencyKey
	if strings.TrimSpace(idempotencyKey) == "" {
		idempotencyKey = ""
	}
	rec := event.Record{
		ID:             id,
		TenantID:       req.TenantID,
		Type:           req.Type,
		Data:           req.Data,
		OccurredAt:     time.Now().UTC(),
		IdempotencyKey: idempotencyKey,
	}
	if err := rec.Validate(); err != nil {
		telemetry.RecordError(span, err, "invalid event")
		metrics.IngestEvents.WithLabelValues("invalid").Inc()
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}
	// The candidate id, overwritten below with the id acceptance actually
	// chose. Set here so a span for a request that never reaches acceptance
	// still names something.
	span.SetAttributes(
		attribute.String("relay.event.id", rec.ID),
		attribute.String("relay.tenant.id", rec.TenantID),
		attribute.String("relay.event.type", rec.Type),
	)

	if s.events == nil {
		err := errors.New("event store is not configured")
		telemetry.RecordError(span, err, "event history unavailable")
		s.log.Error("persist event", "error", "event store is not configured", "event_id", id)
		metrics.IngestEvents.WithLabelValues("unavailable").Inc()
		writeError(w, http.StatusServiceUnavailable, "event history is unavailable, retry")
		return
	}
	acceptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), acceptanceTimeout)
	defer cancel()
	accepted, err := s.events.AcceptEvent(acceptCtx, rec, func(ctx context.Context, chosen event.Record) error {
		ctx, produceSpan := otel.Tracer("relay/ingest").Start(ctx, "kafka.produce",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String("relay.event.id", chosen.ID),
				attribute.String("messaging.destination.name", s.topic),
			),
		)
		defer produceSpan.End()
		value, err := event.EncodeRecord(chosen)
		if err != nil {
			telemetry.RecordError(produceSpan, err, "encode event")
			return fmt.Errorf("encode record: %w", err)
		}
		// No Topic on the message: the Writer carries it, and kafka-go rejects a
		// message that sets it too. The database row is already committed here,
		// so a fast consumer can attach attempt history before this call returns.
		msg := kafka.Message{Key: chosen.Key(), Value: value}
		otel.GetTextMapPropagator().Inject(ctx, telemetry.NewKafkaHeaderCarrier(&msg.Headers))
		if err := s.producer.WriteMessages(ctx, msg); err != nil {
			telemetry.RecordError(produceSpan, err, "publish event")
			return err
		}
		produceSpan.SetStatus(codes.Ok, "")
		return nil
	})
	if err != nil {
		if errors.Is(err, history.ErrIdempotencyConflict) {
			// Never the error itself: it names the caller's idempotency key.
			telemetry.RecordError(span, err, "idempotency conflict")
			metrics.IngestEvents.WithLabelValues("conflict").Inc()
			writeError(w, http.StatusConflict,
				"idempotency key was already used with different type or data")
			return
		}
		operation := "persist event"
		if errors.Is(err, history.ErrPublishFailed) {
			operation = "produce"
		}
		telemetry.RecordError(span, err, operation)
		s.log.Error(operation, "error", err, "event_id", id, "tenant", rec.TenantID)
		metrics.IngestEvents.WithLabelValues("unavailable").Inc()
		writeError(w, http.StatusServiceUnavailable, "could not durably accept the event, retry")
		return
	}
	// Overwrite the candidate id. Acceptance returns the event that already
	// holds this tenant-and-key pair, so a deduplicated request's chosen id is
	// the FIRST request's, not the one generated above -- which is also the id
	// the 202, the Kafka record and every delivery span carry. Tagging the span
	// with the candidate would put an id in the trace that names no event, and
	// would not answer the runbook's query for the id the caller was given.
	span.SetAttributes(
		attribute.String("relay.event.id", accepted.Record.ID),
		attribute.Bool("relay.event.deduplicated", accepted.Deduplicated),
	)
	span.SetStatus(codes.Ok, "")

	if accepted.Deduplicated {
		metrics.IngestEvents.WithLabelValues("deduplicated").Inc()
		s.log.Info("deduplicated", "event_id", accepted.Record.ID,
			"tenant", accepted.Record.TenantID, "type", accepted.Record.Type)
	} else {
		s.accepted.Add(1)
		metrics.IngestEvents.WithLabelValues("accepted").Inc()
		s.log.Info("accepted", "event_id", accepted.Record.ID, "tenant", accepted.Record.TenantID,
			"type", accepted.Record.Type, "topic", s.topic)
	}
	writeJSON(w, http.StatusAccepted, postEventResponse{ID: accepted.Record.ID})
}

type attemptsResponse struct {
	EventID  string            `json:"event_id"`
	Attempts []history.Attempt `json:"attempts"`
}

func (s *Server) getAttempts(w http.ResponseWriter, r *http.Request) {
	if s.events == nil {
		writeError(w, http.StatusServiceUnavailable, "event history is unavailable")
		return
	}
	id := r.PathValue("id")
	attempts, err := s.events.Attempts(r.Context(), id)
	if err != nil {
		if errors.Is(err, history.ErrEventNotFound) {
			writeError(w, http.StatusNotFound, "event %q was not found", id)
			return
		}
		s.log.Error("query attempts", "error", err, "event_id", id)
		writeError(w, http.StatusServiceUnavailable, "event history is unavailable")
		return
	}
	if attempts == nil {
		attempts = make([]history.Attempt, 0)
	}
	writeJSON(w, http.StatusOK, attemptsResponse{EventID: id, Attempts: attempts})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}

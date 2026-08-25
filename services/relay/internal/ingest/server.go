// Package ingest accepts events over HTTP and puts them on the log.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lilabrooks/my-local-platform/relay/internal/event"
)

// MaxBodyBytes bounds a request before it is parsed. Slightly above
// event.MaxDataBytes so an oversized payload is reported as "data too large"
// rather than as a truncated-JSON parse error, which sends the caller looking
// in the wrong place.
const MaxBodyBytes = event.MaxDataBytes + 8*1024

// Producer is the part of kafka.Writer this package needs. An interface so the
// handler is testable without a broker.
type Producer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Server accepts events and produces them.
type Server struct {
	producer Producer
	topic    string
	log      *slog.Logger
	ready    atomic.Bool
	accepted atomic.Int64
}

// New returns a Server that is not yet ready; call MarkReady once the broker
// connection has been established.
func New(p Producer, topic string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{producer: p, topic: topic, log: log}
}

// MarkReady flips readiness. Kubernetes pulls the pod from its Service when
// this goes false, which is how a shutdown drains.
func (s *Server) MarkReady(ready bool) { s.ready.Store(ready) }

// Routes returns the handler.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.postEvent)
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
	body := http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields() // a misspelled field is a caller bug, not a default

	var req postEventRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds %d bytes", MaxBodyBytes)
			return
		}
		writeError(w, http.StatusBadRequest, "malformed JSON: %v", err)
		return
	}

	id, err := event.NewID()
	if err != nil {
		// Losing the random source is not the caller's fault.
		s.log.Error("generate event id", "error", err)
		writeError(w, http.StatusInternalServerError, "could not generate an event id")
		return
	}

	rec := event.Record{
		ID:             id,
		TenantID:       req.TenantID,
		Type:           req.Type,
		Data:           req.Data,
		OccurredAt:     time.Now().UTC(),
		IdempotencyKey: req.IdempotencyKey,
	}
	if err := rec.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "%v", err)
		return
	}

	value, err := json.Marshal(rec)
	if err != nil {
		s.log.Error("encode record", "error", err, "event_id", id)
		writeError(w, http.StatusInternalServerError, "could not encode the event")
		return
	}

	// No Topic on the message: the Writer carries it, and kafka-go rejects a
	// message that sets it too ("Topic must not be specified for both Writer
	// and Message"). Found by running against a real broker -- a fake producer
	// happily accepts both.
	if err := s.producer.WriteMessages(r.Context(), kafka.Message{
		Key:   rec.Key(),
		Value: value,
	}); err != nil {
		// The one outcome that must never happen is returning success for an
		// event that was not durably written, so an unreachable broker is a
		// 503 and the caller retries. Nothing is buffered in memory.
		s.log.Error("produce", "error", err, "event_id", id, "tenant", rec.TenantID)
		writeError(w, http.StatusServiceUnavailable, "could not durably accept the event, retry")
		return
	}

	s.accepted.Add(1)
	s.log.Info("accepted", "event_id", id, "tenant", rec.TenantID, "type", rec.Type, "topic", s.topic)
	writeJSON(w, http.StatusAccepted, postEventResponse{ID: id})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, format string, args ...any) {
	writeJSON(w, status, map[string]string{"error": fmt.Sprintf(format, args...)})
}

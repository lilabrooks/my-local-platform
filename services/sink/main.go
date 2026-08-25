// Command sink is a webhook subscriber that can be told to be slow or to fail.
//
// It is not test scaffolding. A subscriber that answers slowly is what creates
// the consumer lag the M2 autoscaling demo scales on, so this is a first-class
// component of the demo rather than something to delete afterwards.
//
// Signature verification here is written independently of relay's signing,
// against the Standard Webhooks specification, and deliberately shares no code
// with it. If both sides called the same helper, a bug in that helper would
// sign and verify consistently and every test would pass. Two implementations
// of one spec is the only arrangement where agreement means something.
//
// Standard library only, which keeps the image small and the build fast.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Set at build time: -ldflags "-X main.version=..."
var version = "dev"

// toleranceWindow is the Standard Webhooks replay window: a delivery whose
// timestamp is further from now than this is rejected even if it is signed
// correctly, because a valid signature on an old request is a replay.
const toleranceWindow = 5 * time.Minute

// maxBody bounds what a subscriber will read. relay caps events well below it.
const maxBody = 1 << 20

type delivery struct {
	ReceivedAt time.Time       `json:"received_at"`
	Path       string          `json:"path"`
	WebhookID  string          `json:"webhook_id"`
	Type       string          `json:"type"`
	Data       json.RawMessage `json:"data"`
	Status     int             `json:"status"`
}

type sink struct {
	secret []byte

	mu       sync.Mutex
	received []delivery

	// Runtime knobs, so `make relay-demo` can slow the sink mid-run rather
	// than restarting it with different environment variables.
	latencyMS atomic.Int64
	failRate  atomic.Uint64 // float64 bits

	ready   atomic.Bool
	rejects atomic.Int64
}

func main() {
	addr := ":" + envOr("PORT", "8081")

	secret := os.Getenv("RELAY_SIGNING_SECRET")
	if secret == "" {
		// Refuse rather than accept everything. A sink that skips verification
		// would let an unsigned-delivery bug in relay pass every test.
		log.Fatal("RELAY_SIGNING_SECRET is required; local/bootstrap/relay-db.sh prints the seeded value")
	}

	s := &sink{secret: []byte(secret)}
	s.setLatency(parseDurationOr(os.Getenv("SINK_LATENCY"), 0))
	s.setFailRate(parseFloatOr(os.Getenv("SINK_FAIL_RATE"), 0))

	mux := http.NewServeMux()
	// Always succeeds, once the signature checks out.
	mux.HandleFunc("POST /hooks/ok", s.handle(0))
	// Fails at the configured rate; defaults to always, so the dead-letter
	// path is deterministic for the smoke check.
	mux.HandleFunc("POST /hooks/flaky", s.handle(parseFloatOr(os.Getenv("SINK_FLAKY_FAIL_RATE"), 1)))
	mux.HandleFunc("GET /received", s.getReceived)
	mux.HandleFunc("DELETE /received", s.deleteReceived)
	mux.HandleFunc("POST /control", s.postControl)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":   "ready",
			"received": s.count(),
			"rejected": s.rejects.Load(),
		})
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Println("shutdown signal received, failing readiness")
		s.ready.Store(false)
		time.Sleep(3 * time.Second)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}()

	s.ready.Store(true)
	log.Printf("sink %s listening on %s", version, addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
	log.Println("stopped")
}

func (s *sink) handle(failRate float64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
			return
		}

		if err := s.verify(r.Header, body, time.Now()); err != nil {
			s.rejects.Add(1)
			log.Printf("rejected delivery on %s: %v", r.URL.Path, err)
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		// Latency is applied after verification so a slow sink still rejects a
		// forged delivery promptly.
		if d := s.latency(); d > 0 {
			select {
			case <-time.After(d):
			case <-r.Context().Done():
				return
			}
		}

		status := http.StatusOK
		// Per-endpoint rate, plus a global one so the demo can degrade every
		// endpoint at once without restarting.
		if roll(failRate) || roll(s.failRateValue()) {
			status = http.StatusInternalServerError
		}

		var payload struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		_ = json.Unmarshal(body, &payload)

		s.mu.Lock()
		s.received = append(s.received, delivery{
			ReceivedAt: time.Now().UTC(),
			Path:       r.URL.Path,
			WebhookID:  r.Header.Get("webhook-id"),
			Type:       payload.Type,
			Data:       payload.Data,
			Status:     status,
		})
		s.mu.Unlock()

		w.WriteHeader(status)
		_, _ = w.Write([]byte(http.StatusText(status)))
	}
}

// verify implements the Standard Webhooks check: the signed content is
// "{id}.{timestamp}.{body}", the signature is base64 HMAC-SHA256 carried in a
// space-separated list of "v1,<sig>" entries, and a timestamp outside the
// tolerance window is a replay.
func (s *sink) verify(h http.Header, body []byte, now time.Time) error {
	id := h.Get("webhook-id")
	ts := h.Get("webhook-timestamp")
	sig := h.Get("webhook-signature")
	if id == "" || ts == "" || sig == "" {
		return errors.New("missing webhook-id, webhook-timestamp or webhook-signature")
	}

	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("webhook-timestamp %q is not a unix timestamp", ts)
	}
	if delta := now.Sub(time.Unix(secs, 0)); delta > toleranceWindow || delta < -toleranceWindow {
		return fmt.Errorf("webhook-timestamp is %s away from now, outside the %s tolerance", delta.Round(time.Second), toleranceWindow)
	}

	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(id + "." + ts + "."))
	mac.Write(body)
	want := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	// A sender may present several versions; any one matching is enough.
	for _, entry := range strings.Fields(sig) {
		version, encoded, ok := strings.Cut(entry, ",")
		if !ok || version != "v1" {
			continue
		}
		// Constant time: a byte-by-byte comparison leaks how much of a forged
		// signature was correct.
		if subtle.ConstantTimeCompare([]byte(encoded), []byte(want)) == 1 {
			return nil
		}
	}
	return errors.New("no valid v1 signature in webhook-signature")
}

func (s *sink) getReceived(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := append([]delivery(nil), s.received...)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"count": len(out), "deliveries": out})
}

func (s *sink) deleteReceived(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.received = nil
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

type control struct {
	LatencyMS *int64   `json:"latency_ms,omitempty"`
	FailRate  *float64 `json:"fail_rate,omitempty"`
}

// postControl adjusts behaviour at runtime, so the demo can slow this sink and
// watch consumer lag climb without restarting anything.
func (s *sink) postControl(w http.ResponseWriter, r *http.Request) {
	var c control
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&c); err != nil {
		http.Error(w, "malformed JSON", http.StatusBadRequest)
		return
	}
	if c.LatencyMS != nil {
		if *c.LatencyMS < 0 {
			http.Error(w, "latency_ms must not be negative", http.StatusBadRequest)
			return
		}
		s.latencyMS.Store(*c.LatencyMS)
	}
	if c.FailRate != nil {
		if *c.FailRate < 0 || *c.FailRate > 1 {
			http.Error(w, "fail_rate must be between 0 and 1", http.StatusBadRequest)
			return
		}
		s.setFailRate(*c.FailRate)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"latency_ms": s.latencyMS.Load(),
		"fail_rate":  s.failRateValue(),
	})
}

func (s *sink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

func (s *sink) latency() time.Duration     { return time.Duration(s.latencyMS.Load()) * time.Millisecond }
func (s *sink) setLatency(d time.Duration) { s.latencyMS.Store(d.Milliseconds()) }

func (s *sink) setFailRate(f float64)  { s.failRate.Store(math.Float64bits(f)) }
func (s *sink) failRateValue() float64 { return math.Float64frombits(s.failRate.Load()) }

func roll(rate float64) bool {
	switch {
	case rate <= 0:
		return false
	case rate >= 1:
		return true
	default:
		return rand.Float64() < rate
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseDurationOr(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		log.Fatalf("SINK_LATENCY %q: %v", s, err)
	}
	return d
}

func parseFloatOr(s string, def float64) float64 {
	if s == "" {
		return def
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Fatalf("fail rate %q: %v", s, err)
	}
	if f < 0 || f > 1 {
		log.Fatalf("fail rate %v must be between 0 and 1", f)
	}
	return f
}

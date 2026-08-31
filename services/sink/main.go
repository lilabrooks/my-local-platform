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
	"sort"
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

// defaultRetain is how many deliveries the sink keeps.
//
// Sized against the thing that fills it: `make relay-demo` produces 600 events
// per run, so this survives sixteen back-to-back demos before the oldest is
// evicted, and holds ~1.8MB of the container's 128Mi. Large enough that nobody
// tuning a demo has to think about it, small enough that it can never be the
// reason a pod dies.
//
// SINK_RETAIN overrides it. Negative means unbounded, which only the tests ask
// for, and only to prove the bounded path is what stops the growth.
const defaultRetain = 10000

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

// outcome is the label set on sink_deliveries_total. Both fields are bounded --
// two handler paths, a handful of statuses -- so the map cannot grow with
// traffic the way a label carrying an event id would.
type outcome struct {
	path   string
	status int
}

type sink struct {
	secret []byte

	// received is a ring buffer, not a growing slice. It used to be the latter,
	// trimmed only by an explicit DELETE, so a long-lived sink retained every
	// delivery it had ever seen and GET /received serialised all of them on
	// every call -- while the relay smoke check polls it every 250ms.
	//
	// Measured before fixing, to keep the claim proportionate: one delivery
	// retains 184 bytes, so the 128Mi container limit is roughly 729k
	// deliveries, or about 1,200 demo runs. It was a real leak and a real O(n)
	// endpoint, and it was never going to end a demo. See issue #23.
	mu sync.Mutex
	// len(received) IS the retained count -- it grows to retain and then stops,
	// because record() overwrites in place once full. An earlier draft tracked
	// the count in a separate field; a mutation test showed the two could
	// disagree, reporting a bound that was being honoured by the counter and
	// not by the memory.
	received []delivery
	ringNext int // where the next delivery goes once wrapped
	retain   int // negative is unbounded, which only the tests ask for
	outcomes map[outcome]int64

	// Separate from the retained count because DELETE /received resets that,
	// and because eviction does too -- a counter that goes backwards is not a
	// counter. `make relay-replay-verify` clears the history between phases
	// (scripts/verify-replay.sh), so the two genuinely diverge in normal use.
	//
	// An earlier version of this comment said the SMOKE CHECK clears it. That
	// was wrong: services/smoke only ever GETs /received.
	receivedTotal atomic.Int64

	// Runtime knobs, so `make relay-demo` can slow the sink mid-run rather
	// than restarting it with different environment variables.
	latencyMS atomic.Int64
	failRate  atomic.Uint64 // float64 bits

	ready   atomic.Bool
	rejects atomic.Int64

	// A latch parks requests to one path until they are explicitly released.
	//
	// Latency is not a substitute. A verification that slows a subscriber and
	// then races a wall clock -- "it should still be retrying about now" --
	// passes or fails on how loaded the machine is, which is how a CI gate
	// becomes a coin flip. A latch converts that into an observable state: the
	// caller waits until `held` says a request is actually parked, acts, and
	// releases. See scripts/verify-duplicate-on-crash.sh,
	// scripts/verify-graceful-drain.sh and scripts/verify-head-of-line.sh.
	latchMu sync.Mutex
	latched map[string]chan struct{} // path -> closed on release
	held    map[string]int           // path -> requests currently parked
}

// enterLatch reports the release channel for a path, or nil when it is not
// latched. A non-nil return means the caller must call leaveLatch.
func (s *sink) enterLatch(path string) chan struct{} {
	s.latchMu.Lock()
	defer s.latchMu.Unlock()
	ch, ok := s.latched[path]
	if !ok {
		return nil
	}
	s.held[path]++
	return ch
}

func (s *sink) leaveLatch(path string) {
	s.latchMu.Lock()
	defer s.latchMu.Unlock()
	if s.held[path] > 0 {
		s.held[path]--
	}
}

// setLatch arms a path. Arming an already-armed path is a no-op rather than an
// error, so a script that fails partway can re-run without a reset step.
func (s *sink) setLatch(path string) {
	s.latchMu.Lock()
	defer s.latchMu.Unlock()
	if s.latched == nil {
		s.latched = map[string]chan struct{}{}
		s.held = map[string]int{}
	}
	if _, ok := s.latched[path]; !ok {
		s.latched[path] = make(chan struct{})
	}
}

// releaseLatch frees everything parked on a path and disarms it. Releasing a
// path that was never armed is also a no-op, for the same reason.
func (s *sink) releaseLatch(path string) {
	s.latchMu.Lock()
	defer s.latchMu.Unlock()
	if ch, ok := s.latched[path]; ok {
		close(ch)
		delete(s.latched, path)
	}
}

// latchState is what GET /control reports: which paths are armed, and how many
// requests are parked on each right now.
func (s *sink) latchState() (armed []string, held map[string]int) {
	s.latchMu.Lock()
	defer s.latchMu.Unlock()
	held = map[string]int{}
	for p, n := range s.held {
		held[p] = n
	}
	for p := range s.latched {
		armed = append(armed, p)
	}
	sort.Strings(armed)
	return armed, held
}

func main() {
	addr := ":" + envOr("PORT", "8081")

	secret := os.Getenv("RELAY_SIGNING_SECRET")
	if secret == "" {
		// Refuse rather than accept everything. A sink that skips verification
		// would let an unsigned-delivery bug in relay pass every test.
		log.Fatal("RELAY_SIGNING_SECRET is required; local/bootstrap/relay-db.sh prints the seeded value")
	}

	s := &sink{secret: []byte(secret), retain: parseRetain(os.Getenv("SINK_RETAIN"))}
	log.Printf("retaining up to %d deliveries (SINK_RETAIN)", s.retain)
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
	mux.HandleFunc("GET /control", s.getControl)
	mux.HandleFunc("GET /metrics", s.metrics)
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

		// The latch is checked before latency, and after verification for the
		// same reason latency is: a forged delivery is rejected promptly rather
		// than parked. A parked request holds the connection open, which is the
		// point -- relay is mid-delivery and its offset stays uncommitted.
		if release := s.enterLatch(r.URL.Path); release != nil {
			defer s.leaveLatch(r.URL.Path)
			select {
			case <-release:
			case <-r.Context().Done():
				// relay went away mid-request: killed, or shutting down. Not a
				// delivery, and nothing to record.
				return
			}
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

		s.record(delivery{
			ReceivedAt: time.Now().UTC(),
			Path:       r.URL.Path,
			WebhookID:  r.Header.Get("webhook-id"),
			Type:       payload.Type,
			Data:       payload.Data,
			Status:     status,
		}, status)

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

// getReceived serves the retained deliveries, most recent last.
//
// ?limit=N returns only the newest N, so a poller does not pay for the whole
// buffer on every call. The relay smoke check polls this every 250ms looking
// for one webhook id; without a limit it was re-serialising everything the
// sink had ever accepted each time.
//
// `retained` and `total` are reported separately: eviction makes them differ,
// and a caller that conflated them would think deliveries had been lost.
func (s *sink) getReceived(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			http.Error(w, "limit must be a non-negative integer", http.StatusBadRequest)
			return
		}
		limit = n
	}
	out := s.snapshot(limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"count":      len(out),
		"retained":   s.count(),
		"total":      s.receivedTotal.Load(),
		"deliveries": out,
	})
}

func (s *sink) deleteReceived(w http.ResponseWriter, _ *http.Request) {
	s.reset()
	w.WriteHeader(http.StatusNoContent)
}

type control struct {
	LatencyMS *int64   `json:"latency_ms,omitempty"`
	FailRate  *float64 `json:"fail_rate,omitempty"`
	// Latch parks every request to this path until Release names it.
	Latch   *string `json:"latch,omitempty"`
	Release *string `json:"release,omitempty"`
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
	if c.Latch != nil {
		if *c.Latch == "" {
			http.Error(w, "latch must be a path", http.StatusBadRequest)
			return
		}
		s.setLatch(*c.Latch)
	}
	if c.Release != nil {
		if *c.Release == "" {
			http.Error(w, "release must be a path", http.StatusBadRequest)
			return
		}
		s.releaseLatch(*c.Release)
	}
	s.writeControlState(w)
}

// getControl reports current state. The `held` counts are what a verification
// waits on before acting, which is the whole reason the latch is observable
// rather than just a timer somewhere else.
func (s *sink) getControl(w http.ResponseWriter, _ *http.Request) {
	s.writeControlState(w)
}

func (s *sink) writeControlState(w http.ResponseWriter) {
	armed, held := s.latchState()
	writeJSON(w, http.StatusOK, map[string]any{
		"latency_ms": s.latencyMS.Load(),
		"fail_rate":  s.failRateValue(),
		"latched":    armed,
		"held":       held,
	})
}

// metrics emits Prometheus text format by hand, the way services/echo does.
//
// relay took on prometheus/client_golang because it needs a latency histogram
// and per-partition gauges. The sink needs four counters and three gauges, and
// keeping it standard-library-only is a property worth more than the few lines
// this costs: the image stays on scratch and the build stays fast.
//
// Sorted output is not required by the exposition format, but a stable byte
// order makes two scrapes diffable and lets a test assert on the whole body.
func (s *sink) metrics(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	counts := make(map[outcome]int64, len(s.outcomes))
	outcomes := make([]outcome, 0, len(s.outcomes))
	for k, v := range s.outcomes {
		outcomes = append(outcomes, k)
		counts[k] = v
	}
	retained := len(s.received)
	retainLimit := s.retain
	s.mu.Unlock()

	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].path != outcomes[j].path {
			return outcomes[i].path < outcomes[j].path
		}
		return outcomes[i].status < outcomes[j].status
	})

	var b strings.Builder
	writeHeader(&b, "sink_build_info", "gauge",
		"Always 1. Labelled with the version of a running sink process.")
	fmt.Fprintf(&b, "sink_build_info{version=%q} 1\n", version)

	writeHeader(&b, "sink_deliveries_total", "counter",
		"Verified deliveries received, by handler path and the status returned.")
	for _, o := range outcomes {
		fmt.Fprintf(&b, "sink_deliveries_total{path=%q,status=\"%d\"} %d\n", o.path, o.status, counts[o])
	}

	writeHeader(&b, "sink_received_total", "counter",
		"Verified deliveries received since start, across every path.")
	fmt.Fprintf(&b, "sink_received_total %d\n", s.receivedTotal.Load())

	// Retained is deliberately not the same number as received, and the gap is
	// the subject of issue #23: this slice is only ever trimmed by
	// DELETE /received, so under the sustained load M2 generates it grows until
	// the container's 64m limit stops it. Exporting it makes that growth
	// visible rather than an unexplained OOM part-way through a demo.
	writeHeader(&b, "sink_received_retained", "gauge",
		"Deliveries currently held in memory and served by GET /received.")
	fmt.Fprintf(&b, "sink_received_retained %d\n", retained)

	// The bound beside the level, so a panel shows how close eviction is
	// rather than a number with no scale. #23 was hard to judge for exactly
	// that reason: "grows without bound", with nothing to compare against.
	writeHeader(&b, "sink_retain_limit", "gauge",
		"Deliveries retained before the oldest is evicted; negative is unbounded.")
	fmt.Fprintf(&b, "sink_retain_limit %d\n", retainLimit)

	writeHeader(&b, "sink_rejected_total", "counter",
		"Deliveries refused because their signature or timestamp did not check out.")
	fmt.Fprintf(&b, "sink_rejected_total %d\n", s.rejects.Load())

	// The two knobs, exported so a panel can show cause beside effect: the
	// step where latency goes to 2000 is the step where lag starts to climb,
	// and reading that off one graph is most of what the demo is trying to say.
	writeHeader(&b, "sink_latency_ms", "gauge",
		"Configured artificial delay before answering a delivery.")
	fmt.Fprintf(&b, "sink_latency_ms %d\n", s.latencyMS.Load())

	writeHeader(&b, "sink_fail_rate", "gauge",
		"Configured probability that a delivery is answered 500.")
	fmt.Fprintf(&b, "sink_fail_rate %s\n", strconv.FormatFloat(s.failRateValue(), 'g', -1, 64))

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	// One write. The discarded error is the client having hung up mid-response,
	// which there is nothing useful to do about -- discarding it explicitly
	// says that, where ignoring it silently does not.
	_, _ = io.WriteString(w, b.String())
}

// writeHeader emits the HELP and TYPE lines the exposition format wants before
// the first sample of a family.
func writeHeader(b *strings.Builder, name, kind, help string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

// record stores one delivery, evicting the oldest when the buffer is full.
func (s *sink) record(d delivery, status int) {
	s.mu.Lock()
	// Lazily allocated under the same lock that guards them, so a sink built
	// as a bare struct literal -- which the tests do -- cannot panic on a nil
	// map write or retain nothing by accident.
	if s.outcomes == nil {
		s.outcomes = map[outcome]int64{}
	}
	if s.retain == 0 {
		s.retain = defaultRetain
	}

	if s.retain < 0 || len(s.received) < s.retain {
		s.received = append(s.received, d)
	} else {
		s.received[s.ringNext] = d
	}
	if s.retain > 0 {
		s.ringNext = (s.ringNext + 1) % s.retain
	}

	s.outcomes[outcome{path: d.Path, status: status}]++
	s.mu.Unlock()
	s.receivedTotal.Add(1)
}

// snapshot returns up to limit deliveries, oldest first, newest last.
//
// Oldest-first because that is the order they arrived and the order the old
// growing slice returned; a poller asking for "the recent ones" wants the tail
// of that, not a reversed list it has to re-sort.
func (s *sink) snapshot(limit int) []delivery {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]delivery, 0, len(s.received))
	if s.retain < 0 || len(s.received) < s.retain {
		out = append(out, s.received...)
	} else {
		// Wrapped: the oldest entry sits at ringNext.
		out = append(out, s.received[s.ringNext:]...)
		out = append(out, s.received[:s.ringNext]...)
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

func (s *sink) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.received = nil
	s.ringNext = 0
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

// parseRetain reads SINK_RETAIN. Zero is not a legal request -- it would mean
// "retain nothing", which reads as a typo rather than an intention and would
// silently break the smoke check that polls /received for a specific id.
func parseRetain(v string) int {
	if v == "" {
		return defaultRetain
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("SINK_RETAIN %q is not an integer", v)
	}
	if n == 0 {
		log.Fatal("SINK_RETAIN=0 would retain nothing; use a positive count, " +
			"or a negative one for unbounded")
	}
	return n
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

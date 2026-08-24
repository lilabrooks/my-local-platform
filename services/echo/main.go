// Command echo is a small HTTP service used to exercise the deployment path.
//
// It exists so ArgoCD has something real from this repository to deploy rather
// than a stock upstream image. Deliberately dependency-free: the standard
// library only, which keeps the image small and the build fast.
//
// Telemetry is intentionally not wired here. The OTel collector runs in
// docker-compose on the host, not in the cluster, and pretending otherwise
// would mean shipping a config that silently fails. See ADR 0005.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// Set at build time: -ldflags "-X main.version=..."
var version = "dev"

type server struct {
	started  time.Time
	requests atomic.Int64
	ready    atomic.Bool
}

func main() {
	addr := ":" + envOr("PORT", "8080")

	s := &server{started: time.Now()}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("/", s.root)

	srv := &http.Server{
		Addr:              addr,
		Handler:           s.count(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Kubernetes sends SIGTERM, then waits terminationGracePeriodSeconds before
	// SIGKILL. Fail readiness first so the endpoint is pulled from Services
	// before connections stop being accepted.
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
	log.Printf("echo %s listening on %s", version, addr)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
	log.Println("stopped")
}

func (s *server) count(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests.Add(1)
		next.ServeHTTP(w, r)
	})
}

func (s *server) root(w http.ResponseWriter, r *http.Request) {
	host, _ := os.Hostname()
	writeJSON(w, http.StatusOK, map[string]any{
		"service":  "echo",
		"version":  version,
		"pod":      host,
		"path":     r.URL.Path,
		"uptime_s": int(time.Since(s.started).Seconds()),
	})
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) readyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// metrics emits Prometheus text format by hand. Two counters do not justify a
// client library and its dependency tree.
func (s *server) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	// One write rather than six. The discarded error is the client having
	// hung up mid-response, which there is nothing useful to do about --
	// discarding it explicitly says that, where ignoring it silently does not.
	_, _ = fmt.Fprintf(w,
		"# HELP echo_requests_total Total HTTP requests received.\n"+
			"# TYPE echo_requests_total counter\n"+
			"echo_requests_total %d\n"+
			"# HELP echo_uptime_seconds Seconds since process start.\n"+
			"# TYPE echo_uptime_seconds gauge\n"+
			"echo_uptime_seconds %d\n",
		s.requests.Load(), int(time.Since(s.started).Seconds()))
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

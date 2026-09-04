package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lilabrooks/my-local-platform/relay/config"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// listenLocal returns a server bound to a free port, and its base URL.
func listenLocal(t *testing.T, handler http.Handler) (*http.Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return srv, "http://" + ln.Addr().String()
}

// A handler still running when the process decides to shut down must finish
// before the resources it is using are closed.
//
// This is the ordering relay-ingest gets wrong if it returns when
// ListenAndServe returns: Shutdown closes the listener first, ListenAndServe
// returns ErrServerClosed at once, and the caller's deferred closes then pull
// the database pool and the Kafka writer out from under a handler that is still
// persisting an event. postEvent detaches from the request context precisely so
// that work survives a disconnecting client; closing its dependencies destroys
// it just the same.
func TestServeUntilShutdownWaitsForHandlersBeforeReturning(t *testing.T) {
	var (
		mu             sync.Mutex
		handlerDone    bool
		closedTooEarly bool
	)
	release := make(chan struct{})
	inHandler := make(chan struct{})

	srv, base := listenLocal(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(inHandler)
		<-release
		mu.Lock()
		handlerDone = true
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))

	// Stands in for closeBounded: the caller's deferred closes, which run the
	// moment serveUntilShutdown returns.
	closeResources := func() {
		mu.Lock()
		defer mu.Unlock()
		if !handlerDone {
			closedTooEarly = true
		}
	}

	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(base + "/v1/events") //nolint:noctx // the handler blocks on purpose
		if err != nil {
			requestDone <- err
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			requestDone <- fmt.Errorf("status = %d, want 202", resp.StatusCode)
			return
		}
		requestDone <- nil
	}()
	<-inHandler

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() { returned <- serveUntilShutdown(ctx, discardLogger(), srv, func() {}) }()

	cancel() // SIGTERM

	// serveUntilShutdown must still be inside Shutdown here, because the
	// handler has not been released. If it has already returned, the real
	// caller has already closed the pool.
	select {
	case err := <-returned:
		closeResources()
		t.Fatalf("serveUntilShutdown returned (%v) while a handler was still running", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if err := <-requestDone; err != nil {
		t.Fatalf("in-flight request did not complete across shutdown: %v", err)
	}
	if err := <-returned; err != nil {
		t.Fatalf("serveUntilShutdown = %v, want nil", err)
	}
	closeResources()

	mu.Lock()
	defer mu.Unlock()
	if !handlerDone {
		t.Fatal("handler never finished")
	}
	if closedTooEarly {
		t.Fatal("resources were closed before the in-flight handler finished")
	}
}

// A listener that cannot bind must surface as an error, not a silent hang.
func TestServeUntilShutdownReportsAListenerFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Same address, already taken.
	srv := &http.Server{Addr: ln.Addr().String(), ReadHeaderTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- serveUntilShutdown(context.Background(), discardLogger(), srv, func() {}) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("serveUntilShutdown = nil for an unusable listener, want an error")
		}
		if errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("serveUntilShutdown = %v, want the bind failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveUntilShutdown hung on a listener that never started")
	}
}

// The shutdown budget has to outlast the work postEvent detaches from the
// request context, or shutdown destroys exactly what that detachment protects.
// Both constants are in config so this relationship is checkable here rather
// than remembered across two files.
func TestIngestShutdownBudgetCoversTheAcceptanceTimeout(t *testing.T) {
	if config.IngestServerShutdownTimeout <= config.IngestAcceptanceTimeout {
		t.Errorf("IngestServerShutdownTimeout is %v but a detached acceptance may run for %v; "+
			"shutdown would close the pool and the Kafka writer under a handler still persisting an event",
			config.IngestServerShutdownTimeout, config.IngestAcceptanceTimeout)
	}
}

// closeBounded returns even when a closer never does. The process is exiting
// into a grace period with a SIGKILL at the end of it.
func TestCloseBoundedGivesUpOnAStuckCloser(t *testing.T) {
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	var order []string
	var mu sync.Mutex
	record := func(name string) func() error {
		return func() error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}

	start := time.Now()
	closeBounded(discardLogger(), []func() error{
		func() error { <-blocked; return nil }, // registered first, so closed LAST
		record("second"),
		record("third"),
	})
	elapsed := time.Since(start)

	if elapsed < config.ResourceCloseTimeout {
		t.Errorf("closeBounded returned after %v, before its %v budget", elapsed, config.ResourceCloseTimeout)
	}
	if elapsed > config.ResourceCloseTimeout+2*time.Second {
		t.Errorf("closeBounded took %v, well past its %v budget", elapsed, config.ResourceCloseTimeout)
	}

	// Reverse registration order, matching the defer statements this replaced.
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "third" || order[1] != "second" {
		t.Errorf("close order = %v, want [third second]: closeBounded must close in reverse "+
			"registration order, the order the defer statements it replaced used", order)
	}
}

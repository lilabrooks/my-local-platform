// Command smoke verifies every component of the local platform end to end.
//
// It writes and reads back through each one, so a pass means the component
// actually works rather than merely accepting a TCP connection. Exits non-zero
// if any check fails, which makes it usable as a CI gate.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lilabrooks/my-local-platform/smoke/internal/checks"
	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

const checkTimeout = 45 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := platform.Load()
	fmt.Printf("smoke check  %s\n", cfg)

	// Tracing is best effort. If the collector is not running, the checks are
	// still worth running -- losing telemetry should not mask component health.
	tracer, shutdown, err := platform.InitTracing(ctx, cfg)
	if err != nil {
		fmt.Printf("  telemetry disabled: %v\n", err)
	} else {
		defer func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdown(flushCtx); err != nil {
				fmt.Printf("  telemetry flush failed: %v\n", err)
			}
		}()
	}

	list := []checks.Check{
		checks.S3(cfg),
		checks.SNSToSQS(cfg),
		checks.SES(cfg),
		checks.Kafka(cfg),
		checks.RabbitMQ(cfg),
		checks.Postgres(cfg),
		checks.Relay(cfg),
	}

	// One span per check, so a run is one trace. What a span may carry, and
	// why it carries so little, is in checks.Instrument.
	list = checks.Instrument(tracer, list)

	var results []checks.Result
	if tracer != nil {
		rootCtx, root := tracer.Start(ctx, "smoke.run")
		results = checks.Run(rootCtx, checkTimeout, list)
		root.End()
	} else {
		results = checks.Run(ctx, checkTimeout, list)
	}

	if !checks.Report(results) {
		fmt.Println("some checks failed -- `make up` and `make seed`, then retry")
		os.Exit(1)
	}
	fmt.Println("all components healthy")
}

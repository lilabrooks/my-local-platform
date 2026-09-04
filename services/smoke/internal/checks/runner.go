// Package checks exercises each component of the platform end to end.
//
// Every check writes something and then reads it back. A check that only
// connects proves the port is open, which is not the same as the component
// working -- that distinction is the whole point of this service.
package checks

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

// Check is one component probe.
type Check struct {
	Name string
	Run  func(context.Context) (string, error)
}

// Result records the outcome of a single check.
type Result struct {
	Name    string
	Detail  string
	Err     error
	Elapsed time.Duration
}

// OK reports whether the check passed.
func (r Result) OK() bool { return r.Err == nil }

// Run executes checks in order, giving each its own timeout. One failure does
// not stop the others -- when several components are down you want the whole
// picture in a single run, not one error at a time.
func Run(ctx context.Context, timeout time.Duration, list []Check) []Result {
	results := make([]Result, 0, len(list))
	for _, c := range list {
		start := time.Now()
		cctx, cancel := context.WithTimeout(ctx, timeout)
		detail, err := c.Run(cctx)
		cancel()
		results = append(results, Result{
			Name:    c.Name,
			Detail:  detail,
			Err:     err,
			Elapsed: time.Since(start),
		})
	}
	return results
}

// Report prints results and reports whether every check passed.
func Report(results []Result) bool {
	allOK := true
	fmt.Println()
	for _, r := range results {
		status := "\033[32mPASS\033[0m"
		if !r.OK() {
			status = "\033[31mFAIL\033[0m"
			allOK = false
		}
		fmt.Printf("  %s  %-12s %6dms  %s\n", status, r.Name, r.Elapsed.Milliseconds(), r.Detail)
		if !r.OK() {
			fmt.Printf("        %v\n", r.Err)
		}
	}
	fmt.Println()
	return allOK
}

// Instrument wraps each check in a span so one run shows up as a single trace
// in Grafana Tempo (and in Datadog when that exporter is enabled).
//
// The span carries the check's name, its duration, and -- on failure -- a fixed
// description plus a bounded classification. It deliberately carries NOTHING
// derived from the check's own strings.
//
// That restraint is the whole point of this function, and it is why the wrapper
// lives here rather than inline in cmd/smoke: a check's detail and its error are
// built from whatever it saw. The relay check's success detail names the
// dead-lettered subscriber URL; its failures can carry HTTP response bodies and
// decoded event records. A live Tempo trace on 2026-09-03 contained
// `dead-lettered http://sink:8081/hooks/flaky` on a check.detail attribute,
// which is how this was found. Spans leave the host; stdout does not, and
// Report already prints every detail there in full.
//
// This is the same policy relay applies in its own telemetry package. Both are
// enforced by a recorder test rather than by comment.
func Instrument(tracer trace.Tracer, list []Check) []Check {
	if tracer == nil {
		return list
	}
	out := make([]Check, len(list))
	copy(out, list)
	for i, c := range out {
		name, run := c.Name, c.Run
		out[i].Run = func(ctx context.Context) (string, error) {
			ctx, span := tracer.Start(ctx, "check."+name)
			defer span.End()
			detail, err := run(ctx)
			if err != nil {
				// Not span.RecordError: its exception.message is err.Error().
				span.SetAttributes(attribute.String("error.type", platform.ErrorType(err)))
				span.SetStatus(codes.Error, "check failed")
			}
			return detail, err
		}
	}
	return out
}

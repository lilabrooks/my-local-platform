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

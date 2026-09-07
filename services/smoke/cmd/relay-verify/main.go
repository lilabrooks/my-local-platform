// Command relay-verify runs stateful relay verification harnesses.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
	"github.com/lilabrooks/my-local-platform/smoke/internal/relayverify"
)

const usage = "usage: relay-verify ordering [--events N] [--tenant TENANT]"

func main() {
	if len(os.Args) < 2 || os.Args[1] != "ordering" {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}

	opts, err := relayverify.ParseOrderingOptions(os.Args[2:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n%s\n", err, usage)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := relayverify.Ordering(ctx, platform.Load(), opts); err != nil {
		fmt.Fprintf(os.Stderr, "\x1b[31mFAIL\x1b[0m %v\n", err)
		os.Exit(1)
	}
}

//go:build unix

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/lilabrooks/my-local-platform/db-mcp/internal/fixture"
)

func main() {
	root := flag.String("root", ".", "platform repository root")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: db-mcp-fixtures -root PATH up|down")
		os.Exit(2)
	}
	if err := fixture.Run(*root, flag.Arg(0), os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

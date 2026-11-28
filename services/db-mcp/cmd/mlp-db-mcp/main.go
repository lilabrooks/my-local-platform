package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"syscall"

	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func run() error {
	catalogPath := flag.String("catalog", "", "operator-owned TOML catalog")
	flag.Parse()
	if *catalogPath == "" || flag.NArg() != 0 {
		return fmt.Errorf("usage: mlp-db-mcp --catalog path/to/catalog.toml")
	}
	catalog, err := dbmcp.LoadCatalog(*catalogPath)
	if err != nil {
		return err
	}
	caller, err := user.Current()
	if err != nil {
		return fmt.Errorf("identify local process user: %w", err)
	}
	server, err := dbmcp.NewServer(catalog, caller.Username, os.Stderr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Run(ctx, &mcp.StdioTransport{})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

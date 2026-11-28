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
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/pilot"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func run() error {
	catalogPath := flag.String("catalog", "", "operator-owned TOML catalog")
	awsURL := flag.String("aws-url", "", "IAM-authenticated Lambda function URL")
	flag.Parse()
	if (*catalogPath == "") == (*awsURL == "") || flag.NArg() != 0 {
		return fmt.Errorf("choose --catalog path/to/catalog.toml or --aws-url https://<id>.lambda-url.<region>.on.aws")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *awsURL != "" {
		if os.Getenv("MLP_USE_REAL_AWS") != "1" {
			return fmt.Errorf("the signed pilot requires MLP_USE_REAL_AWS=1")
		}
		server, err := pilot.Bridge(ctx, *awsURL)
		if err != nil {
			return err
		}
		return server.Run(ctx, &mcp.StdioTransport{})
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
	return server.Run(ctx, &mcp.StdioTransport{})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

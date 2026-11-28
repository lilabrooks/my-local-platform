package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/pilot"
)

func main() {
	signal.Ignore(syscall.SIGPIPE)
	tools, err := dbmcp.LoadTools()
	if err != nil {
		log.Fatal("invalid database tool contract")
	}
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal("AWS configuration unavailable")
	}
	api := secretsmanager.NewFromConfig(cfg)
	secret := os.Getenv("READER_SECRET_ARN")
	e := pilot.Executor{Tools: tools, Account: os.Getenv("PILOT_ACCOUNT"), ReaderRole: os.Getenv("READER_ROLE_ID"),
		Host: os.Getenv("MYSQL_HOST"), Port: 3306, CAFile: "/var/task/mysql-ca.pem", Audit: dbmcp.NewAuditSink(os.Stdout),
		Password: func(ctx context.Context) (string, error) {
			out, err := api.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &secret})
			if err != nil || out.SecretString == nil || *out.SecretString == "" {
				return "", errors.New("reader credential unavailable")
			}
			return *out.SecretString, nil
		},
	}
	lambda.Start(e.Handle)
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"

	runtime "github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/pilot"
)

func main() {
	tools, err := dbmcp.LoadTools()
	if err != nil {
		log.Fatal("invalid database tool contract")
	}
	var invoke pilot.Invoke
	if arn := os.Getenv("EXECUTOR_ARN"); arn != "" {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			log.Fatal("AWS configuration unavailable")
		}
		api := lambda.NewFromConfig(cfg, func(o *lambda.Options) { o.Retryer = aws.NopRetryer{} })
		invoke = func(ctx context.Context, call pilot.Invocation) (json.RawMessage, error) {
			body, err := json.Marshal(call)
			if err != nil {
				return nil, err
			}
			out, err := api.Invoke(ctx, &lambda.InvokeInput{FunctionName: &arn, Payload: body})
			if err != nil || out.StatusCode != 200 || out.FunctionError != nil {
				return nil, errors.New("private executor unavailable")
			}
			return out.Payload, nil
		}
	}
	router, err := pilot.NewRouter(tools, os.Getenv("PILOT_ACCOUNT"), os.Getenv("READER_ROLE_ID"), os.Getenv("OBSERVER_ROLE_ID"), invoke, os.Stdout)
	if err != nil {
		log.Fatal(err)
	}
	runtime.Start(router.Handle)
}

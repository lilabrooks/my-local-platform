package pilot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var functionURL = regexp.MustCompile(`^https://[a-z0-9]+\.lambda-url\.([a-z0-9-]+)\.on\.aws/?$`)

// Bridge exposes the existing MCP tool contract over stdio. The AWS profile
// stays on the client. A signed request can only go to the configured AWS URL.
func Bridge(ctx context.Context, endpoint string) (*mcp.Server, error) {
	match := functionURL.FindStringSubmatch(endpoint)
	if match == nil {
		return nil, errors.New("expected an HTTPS Lambda function URL")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(match[1]))
	if err != nil {
		return nil, errors.New("could not load AWS client configuration")
	}
	tools, err := dbmcp.LoadTools()
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Timeout: 28 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	client := signedClient{strings.TrimSuffix(endpoint, "/"), match[1], cfg.Credentials, httpClient, v4.NewSigner()}
	return tools.Server(client.call), nil
}

type signedClient struct {
	endpoint, region string
	credentials      aws.CredentialsProvider
	http             *http.Client
	signer           *v4.Signer
}

func (c *signedClient) call(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	if len(args) > 64*1024 {
		return nil, errors.New("tool arguments exceed 64 KiB")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+toolPath+name, bytes.NewReader(args))
	if err != nil {
		return nil, errors.New("invalid AWS request")
	}
	sum := sha256.Sum256(args)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amz-Content-Sha256", hash)
	creds, err := c.credentials.Retrieve(ctx)
	if err != nil {
		return nil, errors.New("AWS credentials unavailable")
	}
	if err := c.signer.SignHTTP(ctx, creds, req, hash, "lambda", c.region, time.Now()); err != nil {
		return nil, errors.New("could not sign the AWS request")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, errors.New("could not reach the AWS pilot")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("AWS pilot returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, dbmcp.ByteLimit+1))
	if err != nil || len(body) > dbmcp.ByteLimit || !json.Valid(body) {
		return nil, errors.New("invalid AWS pilot response")
	}
	return body, nil
}

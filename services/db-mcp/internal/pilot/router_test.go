package pilot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const testAccount = "000000000001"
const testReader = "AROAFIXTUREREADER"
const testObserver = "AROAFIXTUREOBSERVER"
const countArgs = `{"target":"pilot-mysql","database":"meter_demo","sql":"SELECT COUNT(*) AS total FROM readings WHERE state = ?","params":["ready"]}`

func urlEvent(role, name, body string) events.LambdaFunctionURLRequest {
	auth := &events.LambdaFunctionURLRequestContextAuthorizerIAMDescription{AccountID: testAccount, UserID: role + ":session",
		UserARN: "arn:aws:sts::" + testAccount + ":assumed-role/" + role + "/session"}
	return events.LambdaFunctionURLRequest{Version: "2.0", RawPath: toolPath + name, Body: body,
		RequestContext: events.LambdaFunctionURLRequestContext{AccountID: testAccount,
			Authorizer: &events.LambdaFunctionURLRequestContextAuthorizerDescription{IAM: auth},
			HTTP:       events.LambdaFunctionURLRequestContextHTTPDescription{Method: "POST"}}}
}

func newTools(t *testing.T) *dbmcp.Tools {
	t.Helper()
	tools, err := dbmcp.LoadTools()
	if err != nil {
		t.Fatal(err)
	}
	return tools
}

func TestRoleBoundary(t *testing.T) {
	var log bytes.Buffer
	tools := newTools(t)
	r, err := NewRouter(tools, testAccount, testReader, testObserver, nil, &log)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*events.LambdaFunctionURLRequest)
		status int
		code   string
	}{
		{"identity absent", func(e *events.LambdaFunctionURLRequest) { e.RequestContext.Authorizer = nil }, 403, "denied"},
		{"different account", func(e *events.LambdaFunctionURLRequest) { e.RequestContext.Authorizer.IAM.AccountID = "000000000002" }, 403, "denied"},
		{"different ARN account", func(e *events.LambdaFunctionURLRequest) {
			e.RequestContext.Authorizer.IAM.UserARN = "arn:aws:sts::000000000002:assumed-role/test/session"
		}, 403, "denied"},
		{"different session", func(e *events.LambdaFunctionURLRequest) {
			e.RequestContext.Authorizer.IAM.UserID = testReader + ":another"
		}, 403, "denied"},
		{"replaced role", func(e *events.LambdaFunctionURLRequest) {
			e.RequestContext.Authorizer.IAM.UserID = "AROAREPLACEMENT:session"
		}, 403, "denied"},
		{"ambiguous target", func(e *events.LambdaFunctionURLRequest) {
			e.Body = `{"target":"pilot-mysql","target":"other"}`
			e.RawPath = toolPath + "list_databases"
		}, 200, "refused"},
		{"unconfigured executor", func(*events.LambdaFunctionURLRequest) {}, 200, "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := urlEvent(testReader, "run_sql", countArgs)
			tc.change(&e)
			resp, err := r.Handle(t.Context(), e)
			var body struct{ Code string }
			if err != nil || json.Unmarshal([]byte(resp.Body), &body) != nil || resp.StatusCode != tc.status || body.Code != tc.code {
				t.Fatalf("%d %s %v", resp.StatusCode, body.Code, err)
			}
		})
	}
	var called int
	r.invoke = func(_ context.Context, c Invocation) (json.RawMessage, error) {
		called++
		return success("wrong-id", map[string]any{}), nil
	}
	for _, name := range []string{"list_databases", "list_tables", "describe_table", "run_sql", "explain_query", "mongo_find", "mongo_count", "mongo_distinct", "mongo_aggregate"} {
		// The contract validates arguments before the target gate.
		args := map[string]any{"target": TargetName, "database": "meter_demo"}
		switch name {
		case "list_databases":
			delete(args, "database")
		case "describe_table":
			args["table"] = "readings"
		case "run_sql", "explain_query":
			args["sql"] = "SELECT watts FROM readings"
		case "mongo_find", "mongo_count":
			args["collection"] = "readings"
		case "mongo_distinct":
			args["collection"] = "readings"
			args["field"] = "watts"
		case "mongo_aggregate":
			args["collection"] = "readings"
			args["pipeline"] = []any{}
		}
		arg, _ := json.Marshal(args)
		e := urlEvent(testObserver, name, string(arg))
		e.Headers = map[string]string{"x-caller": testReader}
		out, err := r.Handle(t.Context(), e)
		if err != nil || !strings.Contains(out.Body, `"code":"denied"`) || strings.Contains(out.Body, TargetName) {
			t.Errorf("%s: %s", name, out.Body)
		}
	}
	if called != 0 {
		t.Fatal("unentitled calls reached the executor")
	}
	out, err := r.Handle(t.Context(), urlEvent(testReader, "run_sql", countArgs))
	if err != nil || called != 1 || !strings.Contains(out.Body, `"code":"unavailable"`) {
		t.Fatal("mismatched executor response was accepted")
	}
	if strings.Contains(log.String(), "SELECT") || strings.Contains(log.String(), `"ready"`) {
		t.Fatal("query content reached audit")
	}
}

// This test transport verifies the actual Go signer's request at a local TLS
// endpoint using two fixture credentials, then supplies the corresponding URL
// event. Only AWS can establish its live IAM and function invocation behavior.
func signedHarness(t *testing.T, router *Router) func(string) *mcp.ClientSession {
	t.Helper()
	keys := map[string]aws.Credentials{}
	for _, role := range []string{testReader, testObserver} {
		keys[role] = aws.Credentials{AccessKeyID: role, SecretAccessKey: "fixture-only-signing-material", SessionToken: "fixture-token"}
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		date, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
		if err != nil {
			http.Error(w, "bad date", http.StatusForbidden)
			return
		}
		hash := sha256.Sum256(b)
		sum := hex.EncodeToString(hash[:])
		for role, key := range keys {
			candidate, _ := http.NewRequestWithContext(r.Context(), r.Method, "https://"+r.Host+r.URL.RequestURI(), bytes.NewReader(b))
			candidate.Header.Set("Content-Type", "application/json")
			candidate.Header.Set("X-Amz-Content-Sha256", sum)
			err := v4.NewSigner().SignHTTP(r.Context(), key, candidate, sum, "lambda", "us-east-1", date)
			if err != nil || candidate.Header.Get("Authorization") != r.Header.Get("Authorization") || sum != r.Header.Get("X-Amz-Content-Sha256") || r.Header.Get("X-Amz-Security-Token") != key.SessionToken {
				continue
			}
			e := urlEvent(role, strings.TrimPrefix(r.URL.Path, toolPath), string(b))
			resp, err := router.Handle(r.Context(), e)
			if err != nil {
				http.Error(w, "router error", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = io.WriteString(w, resp.Body)
			return
		}
		http.Error(w, "signature rejected", http.StatusForbidden)
	}))
	t.Cleanup(server.Close)
	return func(role string) *mcp.ClientSession {
		k := keys[role]
		client := signedClient{server.URL, "us-east-1", credentials.NewStaticCredentialsProvider(k.AccessKeyID, k.SecretAccessKey, k.SessionToken), server.Client(), v4.NewSigner()}
		s := router.tools.Server(client.call)
		a, b := mcp.NewInMemoryTransports()
		ss, err := s.Connect(t.Context(), a, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ss.Close() })
		c := mcp.NewClient(&mcp.Implementation{Name: "pilot-test", Version: "1"}, nil)
		cs, err := c.Connect(t.Context(), b, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cs.Close() })
		return cs
	}
}

func toolCall(t *testing.T, client *mcp.ClientSession, name, args, outcome string) map[string]any {
	t.Helper()
	result, err := client.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: json.RawMessage(args)})
	if err != nil || len(result.Content) != 1 {
		t.Fatalf("%s: bad protocol result: %v", name, err)
	}
	content, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("%s: expected text content", name)
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(content.Text), &body); err != nil {
		t.Fatal(err)
	}
	if result.IsError != (outcome != "ok") || body["ok"] != (outcome == "ok") || outcome != "ok" && body["code"] != outcome {
		t.Fatalf("%s: isError=%t, ok=%v, code=%v, want %s", name, result.IsError, body["ok"], body["code"], outcome)
	}
	if id, _ := body["request_id"].(string); !requestIDPattern.MatchString(id) {
		t.Fatal("missing request ID")
	}
	return body
}

func TestSignedDiscovery(t *testing.T) {
	var audit bytes.Buffer
	r, err := NewRouter(newTools(t), testAccount, testReader, testObserver, nil, &audit)
	if err != nil {
		t.Fatal(err)
	}
	client := signedHarness(t, r)
	for _, role := range []string{testReader, testObserver} {
		session := client(role)
		me := toolCall(t, session, "whoami", "{}", "ok")
		if me["transport"] != "aws-iam" || !strings.Contains(me["user"].(string), "/"+role+"/") {
			t.Fatal(me)
		}
		targets := toolCall(t, session, "list_targets", "{}", "ok")["targets"].([]any)
		if role == testReader && len(targets) != 1 || role == testObserver && len(targets) != 0 {
			t.Fatal(targets)
		}
	}
}

func TestExecutorRejectsBeforeSecret(t *testing.T) {
	accessed := false
	e := Executor{Tools: newTools(t), Account: testAccount, ReaderRole: testReader, Password: func(context.Context) (string, error) { accessed = true; return "", errors.New("unused") }}
	request := Invocation{strings.Repeat("0", 32), "arn:aws:sts::" + testAccount + ":assumed-role/test/session", testObserver, "run_sql", json.RawMessage(countArgs)}
	if _, err := e.Handle(t.Context(), request); err == nil {
		t.Fatal("observer reached the reader")
	}
	request.RoleID = testReader
	request.Arguments = json.RawMessage(strings.ReplaceAll(countArgs, TargetName, "unknown"))
	if _, err := e.Handle(t.Context(), request); err == nil || accessed {
		t.Fatal("invalid target reached secret retrieval")
	}
}

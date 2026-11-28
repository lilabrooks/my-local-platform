package dbmcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type observed struct {
	Operation, Outcome, Target string
	Bytes                      int
}
type stdioClient struct {
	session *mcp.ClientSession
	logs    *os.File
	calls   map[string]observed
}

func connectTestClient(t *testing.T, cmd *exec.Cmd) *stdioClient {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "audit")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	cmd.Stderr = file
	c := mcp.NewClient(&mcp.Implementation{Name: "db-mcp-test-client", Version: "1"}, nil)
	session, err := c.Connect(t.Context(), &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		out, _ := os.ReadFile(file.Name())
		t.Fatalf("connect: %v (%s)", err, out)
	}
	t.Cleanup(func() { _ = session.Close() })
	return &stdioClient{session: session, logs: file, calls: map[string]observed{}}
}
func (c *stdioClient) call(t *testing.T, target, tool string, args any, ok bool) document {
	t.Helper()
	outcome := "ok"
	if !ok {
		outcome = "refused"
	}
	return c.callOutcome(t, target, tool, args, outcome)
}
func (c *stdioClient) callOutcome(t *testing.T, target, tool string, args any, outcome string) document {
	t.Helper()
	result, err := c.session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil || result.IsError || len(result.Content) != 1 {
		t.Fatalf("%s: bad protocol result: %v", tool, err)
	}
	text, valid := result.Content[0].(*mcp.TextContent)
	if !valid || len(text.Text) > ByteLimit {
		t.Fatalf("%s: invalid content", tool)
	}
	var payload document
	if err := json.Unmarshal([]byte(text.Text), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["ok"] != (outcome == "ok") || (outcome != "ok" && payload["code"] != outcome) {
		t.Fatalf("%s: unexpected reply %v", tool, payload)
	}
	id, _ := payload["request_id"].(string)
	if _, seen := c.calls[id]; id == "" || seen {
		t.Fatal("missing or repeated request ID")
	}
	c.calls[id] = observed{Operation: tool, Outcome: outcome, Target: target, Bytes: len(text.Text)}
	return payload
}
func (c *stdioClient) checkAudit(t *testing.T) map[string]auditRecord {
	t.Helper()
	if err := c.session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.logs.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(c.logs)
	seen := map[string]auditRecord{}
	for lines.Scan() {
		var e auditRecord
		var fields map[string]any
		if err := json.Unmarshal(lines.Bytes(), &fields); err != nil {
			t.Fatalf("unexpected log: %s", lines.Text())
		}
		if err := json.Unmarshal(lines.Bytes(), &e); err != nil {
			t.Fatal(err)
		}
		for key := range fields {
			if !slices.Contains([]string{"request_id", "operation", "caller", "outcome", "target", "object", "rows", "bytes", "elapsed_ms"}, key) {
				t.Errorf("unexpected audit field %s", key)
			}
		}
		call, ok := c.calls[e.ID]
		if !ok || seen[e.ID].ID != "" || call.Operation != e.Operation || call.Outcome != e.Outcome ||
			call.Target != e.Target || call.Bytes != e.Bytes || e.Caller == "" {
			t.Errorf("uncorrelated record: %+v", e)
		}
		seen[e.ID] = e
	}
	if err := lines.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != len(c.calls) {
		t.Fatalf("%d audit records for %d calls", len(seen), len(c.calls))
	}
	t.Logf("%d MCP requests matched individual metadata-only audit records", len(seen))
	return seen
}

// Start a real SDK stdio process during ordinary go test, including an empty catalog.
func TestStdioChild(t *testing.T) {
	path := os.Getenv("MLP_DB_MCP_TEST_CATALOG")
	if path == "" {
		t.Skip("subprocess entry point")
	}
	cat, err := LoadCatalog(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	s, err := NewServer(cat, "fixture-user", os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = s.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
func TestEmptyCatalogOverStdio(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.toml")
	if err := os.WriteFile(path, []byte("targets = []\n"), 0600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestStdioChild$")
	cmd.Env = append(os.Environ(), "MLP_DB_MCP_TEST_CATALOG="+path)
	c := connectTestClient(t, cmd)
	list, err := c.session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tool := range list.Tools {
		if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint || tool.Annotations.DestructiveHint == nil || *tool.Annotations.DestructiveHint {
			t.Fatalf("unexpected annotations: %s", tool.Name)
		}
		names[tool.Name] = true
	}
	want := []string{"describe_table", "explain_query", "list_databases", "list_tables", "list_targets", "mongo_aggregate", "mongo_count", "mongo_distinct", "mongo_find", "run_sql", "whoami"}
	if !slices.Equal(slices.Sorted(maps.Keys(names)), want) {
		t.Fatal(names)
	}
	targets := c.call(t, "", "list_targets", document{}, true)["targets"].([]any)
	if targets == nil || len(targets) != 0 {
		t.Fatal(targets)
	}
	c.call(t, "", "list_databases", document{"target": "sentinel-excluded"}, false)
	c.call(t, "", "run_sql", document{"target": "x"}, false)
	c.checkAudit(t)
	data, _ := os.ReadFile(c.logs.Name())
	if strings.Contains(string(data), "sentinel-excluded") {
		t.Fatal("audit included unknown target")
	}
}

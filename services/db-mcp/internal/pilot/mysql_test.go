//go:build integration

package pilot

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
)

func TestPilotMySQL(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("PILOT_TEST_PORT"))
	if err != nil {
		t.Fatal("run the packaged pilot rehearsal")
	}
	var routerLog, executorLog bytes.Buffer
	lookups := 0
	e := Executor{Tools: newTools(t), Account: testAccount, ReaderRole: testReader, Host: "127.0.0.1", Port: port,
		CAFile: os.Getenv("PILOT_TEST_CA"), Audit: dbmcp.NewAuditSink(&executorLog), Password: func(context.Context) (string, error) {
			lookups++
			b, err := os.ReadFile(os.Getenv("PILOT_TEST_READER"))
			return string(b), err
		}}
	r, err := NewRouter(e.Tools, testAccount, testReader, testObserver, e.Handle, &routerLog)
	if err != nil {
		t.Fatal(err)
	}
	sessions := signedHarness(t, r)
	reader, observer := sessions(testReader), sessions(testObserver)
	count := toolCall(t, reader, "run_sql", countArgs, "ok")
	rows, _ := json.Marshal(count["rows"])
	if string(rows) != "[[32]]" {
		t.Fatalf("unexpected count: %s, %v", rows, count["code"])
	}
	observerDenial := toolCall(t, observer, "run_sql", countArgs, "denied")
	if lookups != 1 {
		t.Fatal("observer reached reader credential")
	}
	const excludedArgs = `{"target":"pilot-mysql","database":"meter_demo","sql":"SELECT watts FROM meter_events"}`
	refusal := toolCall(t, reader, "run_sql", excludedArgs, "refused")

	// Allow the same object in a test catalog while retaining the database user's real grants.
	password, err := e.Password(t.Context())
	if err != nil {
		t.Fatal("fixture reader credential unavailable")
	}
	catalog, err := dbmcp.NewMySQLReader(TargetName, e.Host, e.Port, e.CAFile, password)
	if err != nil {
		t.Fatal(err)
	}
	catalog.Targets[0].Objects["meter_events"] = []string{"watts"}
	nativeRouter, err := NewRouter(e.Tools, testAccount, testReader, testObserver, func(ctx context.Context, call Invocation) (json.RawMessage, error) {
		return e.Tools.Execute(ctx, catalog, e.Audit.Reserve(), call.Caller, call.RequestID, call.Tool, call.Arguments)
	}, &routerLog)
	if err != nil {
		t.Fatal(err)
	}
	nativeDenial := toolCall(t, signedHarness(t, nativeRouter)(testReader), "run_sql", excludedArgs, "denied")
	if nativeDenial["message"] != "The configured database reader lacks permission for this operation." {
		t.Fatal("unexpected public error for a native MySQL grant denial")
	}
	e.CAFile = os.Getenv("PILOT_TEST_OTHER_CA")
	unavailable := toolCall(t, reader, "run_sql", countArgs, "unavailable")
	wantExecutor := map[string]string{
		count["request_id"].(string):        "ok",
		refusal["request_id"].(string):      "refused",
		nativeDenial["request_id"].(string): "denied",
		unavailable["request_id"].(string):  "unavailable",
	}
	assertReadAudit(t, &executorLog, wantExecutor)
	wantRouter := maps.Clone(wantExecutor)
	wantRouter[observerDenial["request_id"].(string)] = "denied"
	assertReadAudit(t, &routerLog, wantRouter)
	for _, log := range []string{routerLog.String(), executorLog.String()} {
		if strings.Contains(log, "SELECT") || strings.Contains(log, `"ready"`) || strings.Contains(log, "[[32]]") {
			t.Fatal("query payload appears in audit")
		}
	}
	t.Log("signed MCP reader counted 32 over verified MySQL TLS; catalog, native grant, observer and unrelated CA outcomes matched their audit IDs")
}

func assertReadAudit(t *testing.T, log *bytes.Buffer, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	for line := range bytes.Lines(log.Bytes()) {
		var event auditEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if _, duplicate := got[event.RequestID]; duplicate || event.Caller == "" || event.Operation != "run_sql" {
			t.Fatal("invalid or duplicate read audit event")
		}
		got[event.RequestID] = event.Outcome
	}
	if !maps.Equal(got, want) {
		t.Fatalf("read audit outcomes %v, want %v", got, want)
	}
}

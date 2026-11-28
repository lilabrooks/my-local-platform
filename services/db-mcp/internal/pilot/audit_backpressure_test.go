package pilot

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lilabrooks/my-local-platform/db-mcp/internal/dbmcp"
)

type stalledPilotAudit struct {
	release chan struct{}
	lines   chan []byte
}

func newStalledPilotAudit(t *testing.T) (*stalledPilotAudit, func()) {
	t.Helper()
	w := &stalledPilotAudit{release: make(chan struct{}), lines: make(chan []byte, 5)}
	resume := sync.OnceFunc(func() { close(w.release) })
	t.Cleanup(resume)
	return w, resume
}

func (w *stalledPilotAudit) Write(line []byte) (int, error) {
	<-w.release
	w.lines <- bytes.Clone(line)
	return len(line), nil
}

func (w *stalledPilotAudit) expect(t *testing.T, requestID, operation, outcome, auditState string) {
	t.Helper()
	select {
	case line := <-w.lines:
		var event auditEvent
		if json.Unmarshal(line, &event) != nil || event.RequestID != requestID || event.Caller == "" || event.Operation != operation ||
			event.Target != TargetName || event.Outcome != outcome || event.AuditState != auditState ||
			bytes.Contains(line, []byte("SELECT")) || bytes.Contains(line, []byte("fixture value")) {
			t.Fatalf("wrong audit metadata: %s", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("missing recovered audit entry")
	}
}

func waitForAuditRecovery(t *testing.T, sink *dbmcp.AuditSink) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slot := sink.Reserve(); slot != nil {
			slot.Release()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("audit admission did not recover")
}

func TestSignedRouterWithholdsResultsOnAuditFailure(t *testing.T) {
	for _, attempted := range []string{"ok", "unavailable"} {
		t.Run(attempted, func(t *testing.T) {
			w, resume := newStalledPilotAudit(t)
			var dispatches atomic.Int32
			r, err := NewRouter(newTools(t), testAccount, testReader, testObserver, func(_ context.Context, call Invocation) (json.RawMessage, error) {
				dispatches.Add(1)
				if attempted == "ok" {
					return success(call.RequestID, map[string]any{"rows": []string{"fixture value"}}), nil
				}
				return failure(call.RequestID, "unavailable", "Fixture database unavailable."), nil
			}, w)
			if err != nil {
				t.Fatal(err)
			}
			reader := signedHarness(t, r)(testReader)
			started := time.Now()
			first := toolCall(t, reader, "run_sql", countArgs, "unavailable")
			elapsed := time.Since(started)
			if elapsed < dbmcp.AuditWriteTimeout || elapsed > 2*time.Second ||
				dispatches.Load() != 1 || first["message"] != "Required audit is unavailable." || first["rows"] != nil {
				t.Fatalf("first dispatches=%d, elapsed=%s, result=%v", dispatches.Load(), elapsed, first)
			}
			started = time.Now()
			second := toolCall(t, reader, "run_sql", countArgs, "unavailable")
			refusal := time.Since(started)
			who := toolCall(t, reader, "whoami", "{}", "unavailable")
			if dispatches.Load() != 1 || second["request_id"] == first["request_id"] || who["user"] != nil {
				t.Fatal("known audit outage allowed dispatch or disclosed caller metadata")
			}
			noIdentity := urlEvent(testReader, "run_sql", countArgs)
			noIdentity.RequestContext.Authorizer = nil
			denied, err := r.Handle(t.Context(), noIdentity)
			if err != nil || denied.StatusCode != http.StatusForbidden || !bytes.Contains([]byte(denied.Body), []byte(`"code":"denied"`)) {
				t.Fatal("unauthenticated caller did not retain its forbidden response")
			}
			resume()
			id := first["request_id"].(string)
			w.expect(t, id, "run_sql", attempted, "")
			w.expect(t, id, "run_sql", "unavailable", "response_withheld")
			waitForAuditRecovery(t, r.audit)
			again := toolCall(t, reader, "run_sql", countArgs, attempted)
			w.expect(t, again["request_id"].(string), "run_sql", attempted, "")
			if dispatches.Load() != 2 {
				t.Fatal("router did not resume dispatch")
			}
			t.Logf("signed post-dispatch refusal %s, pre-dispatch refusal %s", elapsed.Round(time.Millisecond), refusal.Round(time.Microsecond))
		})
	}
}

func TestSignedExecutorRefusesBeforeCredentialDuringAuditOutage(t *testing.T) {
	w, resume := newStalledPilotAudit(t)
	var lookups atomic.Int32
	executor := Executor{Tools: newTools(t), Account: testAccount, ReaderRole: testReader, Host: "127.0.0.1",
		Port: 3306, CAFile: "/fixture/no-connection-needed.pem", Audit: dbmcp.NewAuditSink(w),
		Password: func(context.Context) (string, error) {
			lookups.Add(1)
			return "fixture", nil
		}}
	var routerAudit bytes.Buffer
	r, err := NewRouter(executor.Tools, testAccount, testReader, testObserver, executor.Handle, &routerAudit)
	if err != nil {
		t.Fatal(err)
	}
	reader := signedHarness(t, r)(testReader)
	const args = `{"target":"pilot-mysql"}`
	first := toolCall(t, reader, "list_databases", args, "unavailable")
	second := toolCall(t, reader, "list_databases", args, "unavailable")
	if lookups.Load() != 1 || first["databases"] != nil || second["databases"] != nil || first["request_id"] == second["request_id"] {
		t.Fatal("known executor audit outage released data or reloaded the credential")
	}
	for _, result := range []map[string]any{first, second} {
		if !bytes.Contains(routerAudit.Bytes(), []byte(result["request_id"].(string))) {
			t.Fatal("healthy upstream audit lost the refused request ID")
		}
	}
	resume()
	id := first["request_id"].(string)
	w.expect(t, id, "list_databases", "ok", "")
	w.expect(t, id, "list_databases", "unavailable", "response_withheld")
	waitForAuditRecovery(t, executor.Audit)
	ok := toolCall(t, reader, "list_databases", args, "ok")
	w.expect(t, ok["request_id"].(string), "list_databases", "ok", "")
	if lookups.Load() != 2 || ok["databases"] == nil {
		t.Fatal("executor did not resume normal metadata access")
	}
}

func TestRouterAuditOutagePreservesUnauthenticatedDenial(t *testing.T) {
	w, resume := newStalledPilotAudit(t)
	r, err := NewRouter(newTools(t), testAccount, testReader, testObserver, nil, w)
	if err != nil {
		t.Fatal(err)
	}
	e := urlEvent(testReader, "whoami", "{}")
	e.RequestContext.Authorizer = nil
	response, err := r.Handle(t.Context(), e)
	var result struct {
		ID   string `json:"request_id"`
		Code string `json:"code"`
	}
	if err != nil || response.StatusCode != http.StatusForbidden || json.Unmarshal([]byte(response.Body), &result) != nil ||
		result.Code != "denied" || !requestIDPattern.MatchString(result.ID) {
		t.Fatal("unverified identity did not retain its original denial")
	}
	resume()
	for _, marker := range []string{"", "response_denied"} {
		select {
		case line := <-w.lines:
			var event auditEvent
			if json.Unmarshal(line, &event) != nil || event.RequestID != result.ID || event.Outcome != "denied" ||
				event.Caller != "" || event.RoleID != "" || event.AuditState != marker {
				t.Fatalf("unexpected denial audit: %s", line)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("missing recovered denial")
		}
	}
}

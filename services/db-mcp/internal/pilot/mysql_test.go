//go:build integration

package pilot

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestPilotMySQL(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("PILOT_TEST_PORT"))
	if err != nil {
		t.Fatal("run the packaged pilot rehearsal")
	}
	var routerLog, executorLog bytes.Buffer
	lookups := 0
	e := Executor{Tools: newTools(t), Account: testAccount, ReaderRole: testReader, Host: "127.0.0.1", Port: port,
		CAFile: os.Getenv("PILOT_TEST_CA"), Audit: &executorLog, Password: func(context.Context) (string, error) {
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
	count := toolCall(t, reader, "run_sql", countArgs)
	rows, _ := json.Marshal(count["rows"])
	if count["ok"] != true || string(rows) != "[[32]]" {
		t.Fatalf("unexpected count: %s, %v", rows, count["code"])
	}
	id := count["request_id"].(string)
	if !strings.Contains(routerLog.String(), id) || !strings.Contains(executorLog.String(), id) {
		t.Fatal("successful read lacks correlated audit")
	}
	denial := toolCall(t, observer, "run_sql", countArgs)
	if denial["code"] != "denied" || !strings.Contains(routerLog.String(), denial["request_id"].(string)) ||
		strings.Contains(executorLog.String(), denial["request_id"].(string)) || lookups != 1 {
		t.Fatal("observer reached reader credential or lacked a denial audit")
	}
	e.CAFile = os.Getenv("PILOT_TEST_OTHER_CA")
	if bad := toolCall(t, reader, "run_sql", countArgs); bad["code"] != "unavailable" {
		t.Fatal("reader accepted an unrelated CA")
	}
	for _, log := range []string{routerLog.String(), executorLog.String()} {
		if strings.Contains(log, "SELECT") || strings.Contains(log, `"ready"`) || strings.Contains(log, "[[32]]") {
			t.Fatal("query payload appears in audit")
		}
	}
	t.Log("signed MCP reader counted 32 over verified MySQL TLS; observer denied before secret access; audit IDs matched; unrelated CA rejected")
}

//go:build integration && unix

package dbmcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestDemoAuditPipe(t *testing.T) {
	openPipe := func(t *testing.T) (*os.File, *os.File) {
		t.Helper()
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reader.Close() })
		t.Cleanup(func() { _ = writer.Close() })
		return reader, writer
	}

	t.Run("recovery", func(t *testing.T) {
		reader, writer := openPipe(t)
		padding := fillAuditPipe(t, writer)
		session, closeServer := connectAuditDemo(t, writer)
		defer closeServer()
		query := document{"target": "demo-mysql", "database": "meter_demo", "sql": "SELECT COUNT(*) FROM readings WHERE state=?", "params": []any{"ready"}}
		withheld := callAuditDemo(t, session, "run_sql", query)
		if withheld["code"] != "unavailable" {
			t.Fatal("fixture read returned data while its audit destination was stalled")
		}
		if denied := callAuditDemo(t, session, "whoami", document{}); denied["code"] != "unavailable" {
			t.Fatal("metadata was disclosed while the audit destination was stalled")
		}

		if err := reader.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.CopyN(io.Discard, reader, padding); err != nil {
			t.Fatal(err)
		}
		records := json.NewDecoder(reader)
		records.DisallowUnknownFields()
		var rec auditRecord
		// The original write can finish after the response was withheld. Its
		// terminal record must identify the same request and disclose no rows.
		for range 2 {
			if err := records.Decode(&rec); err != nil {
				t.Fatal(err)
			}
			if rec.ID != withheld["request_id"] || rec.Operation != "run_sql" {
				t.Fatalf("unexpected audit record after drain: %+v", rec)
			}
			if rec.Outcome == "unavailable" {
				break
			}
			if rec.Outcome != "ok" || rec.AuditState != "" || rec.Rows == nil || *rec.Rows != 1 {
				t.Fatalf("fixture query did not complete before its response was withheld: %+v", rec)
			}
			rec = auditRecord{}
		}
		if rec.Outcome != "unavailable" || rec.AuditState != "response_withheld" || rec.Rows != nil {
			t.Fatalf("missing terminal failure record: %+v", rec)
		}
		// A reader can observe the correction just before the writer resumes
		// accepting requests. Allow a short, bounded retry for that handoff.
		response := callAuditDemo(t, session, "run_sql", query)
		for _, delay := range []time.Duration{25 * time.Millisecond, 100 * time.Millisecond, 400 * time.Millisecond} {
			if response["ok"] == true {
				break
			}
			time.Sleep(delay)
			response = callAuditDemo(t, session, "run_sql", query)
		}
		checkJSON(t, response["rows"], `[[32]]`)
		rec = auditRecord{}
		if err := records.Decode(&rec); err != nil {
			t.Fatal(err)
		}
		if rec.ID != response["request_id"] || rec.Outcome != "ok" || rec.AuditState != "" || rec.Operation != "run_sql" ||
			rec.Target != "demo-mysql" || rec.Object != "readings" || rec.Rows == nil || *rec.Rows != 1 {
			t.Fatalf("recovered fixture read did not match its audit record: %+v", rec)
		}
	})

	t.Run("shutdown while stalled", func(t *testing.T) {
		_, writer := openPipe(t)
		fillAuditPipe(t, writer)
		session, closeServer := connectAuditDemo(t, writer)
		defer closeServer() // leave the reader open and undrained until the server exits
		if got := callAuditDemo(t, session, "whoami", document{}); got["code"] != "unavailable" {
			t.Fatal("stalled pipe returned metadata")
		}
	})

	t.Run("disconnected reader", func(t *testing.T) {
		reader, writer := openPipe(t)
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		session, closeServer := connectAuditDemo(t, writer)
		defer closeServer()
		if got := callAuditDemo(t, session, "whoami", document{}); got["code"] != "unavailable" {
			t.Fatal("disconnected pipe returned metadata")
		}
	})
}

// Fill the test pipe before starting the server so a single ordinary request
// encounters backpressure independently of the kernel's pipe capacity.
func fillAuditPipe(t *testing.T, writer *os.File) int64 {
	t.Helper()
	fd := int(writer.Fd())
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	var padding [4096]byte
	var total int64
	for _, chunk := range [][]byte{padding[:], padding[:1]} {
		for {
			n, err := syscall.Write(fd, chunk)
			if errors.Is(err, syscall.EAGAIN) {
				break
			}
			if err != nil || n <= 0 || total >= 1<<20 {
				t.Fatalf("could not fill synthetic pipe at %d bytes: %v", total, err)
			}
			total += int64(n)
		}
	}
	if err := syscall.SetNonblock(fd, false); err != nil {
		t.Fatal(err)
	}
	return total
}

func connectAuditDemo(t *testing.T, writer *os.File) (*mcp.ClientSession, func()) {
	t.Helper()
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, filepath.Join(root, "bin", "mlp-db-mcp"), "--catalog", filepath.Join(root, "services", "db-mcp", "demo.toml"))
	cmd.Stderr = writer
	client := mcp.NewClient(&mcp.Implementation{Name: "audit-pipe-test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.CommandTransport{Command: cmd, TerminateDuration: 4 * time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return session, func() {
		start := time.Now()
		err := session.Close()
		elapsed := time.Since(start)
		if err != nil || cmd.ProcessState == nil || !cmd.ProcessState.Success() || elapsed >= 3*time.Second {
			t.Errorf("server did not exit normally on stdin EOF: %s, %v, %v", elapsed, cmd.ProcessState, err)
		}
		t.Logf("stdio server exited in %s", elapsed)
	}
}

func callAuditDemo(t *testing.T, session *mcp.ClientSession, name string, args document) document {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	start := time.Now()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil || result == nil || len(result.Content) != 1 {
		t.Fatalf("%s did not return an MCP tool result: %v", name, err)
	}
	content, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("%s returned unexpected content", name)
	}
	var reply document
	if err := json.Unmarshal([]byte(content.Text), &reply); err != nil {
		t.Fatal(err)
	}
	id, _ := reply["request_id"].(string)
	if id == "" || reply["ok"] != !result.IsError {
		t.Fatalf("%s returned an inconsistent MCP tool result", name)
	}
	if result.IsError {
		if reply["code"] != "unavailable" || reply["message"] != "Required audit is unavailable." || len(reply) != 4 {
			t.Fatalf("%s returned an unexpected error envelope: %v", name, reply)
		}
	}
	t.Logf("%s returned ok=%t in %s", name, !result.IsError, time.Since(start))
	return reply
}

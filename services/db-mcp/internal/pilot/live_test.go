//go:build awspilot

package pilot

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Both the CLI and the MCP binary must resolve the selected shared profile.
func liveProfileEnv(profile string) []string {
	return []string{"HOME=" + os.Getenv("HOME"), "PATH=" + os.Getenv("PATH"), "AWS_PROFILE=" + profile,
		"AWS_REGION=us-east-1", "AWS_DEFAULT_REGION=us-east-1", "AWS_EC2_METADATA_DISABLED=true", "AWS_PAGER=", "MLP_USE_REAL_AWS=1"}
}

// This opt-in test uses a deployed candidate. It does not provision resources.
func TestLivePilot(t *testing.T) {
	phase := os.Getenv("PILOT_PHASE")
	endpoint := os.Getenv("PILOT_URL")
	operator := os.Getenv("PILOT_OPERATOR_PROFILE")
	if os.Getenv("MLP_USE_REAL_AWS") != "1" || (phase != "identity" && phase != "hourly") ||
		!functionURL.MatchString(endpoint) || operator == "" {
		t.Fatal("set the explicit live opt-in, signed URL, phase and audit operator profile")
	}
	binary, err := filepath.Abs("../../../../bin/mlp-db-mcp")
	if err != nil {
		t.Fatal(err)
	}
	var expected []auditEvent
	for _, role := range []string{"reader", "observer"} {
		t.Run(role, func(t *testing.T) {
			profile := os.Getenv("PILOT_" + strings.ToUpper(role) + "_PROFILE")
			if profile == "" {
				t.Fatal("supply an AWS CLI profile for this pilot role")
			}
			env := liveProfileEnv(profile)
			cli := func(args ...string) ([]byte, error) {
				cmd := exec.CommandContext(t.Context(), "aws", args...)
				cmd.Env = env
				return cmd.CombinedOutput()
			}
			b, err := cli("sts", "get-caller-identity", "--query", "Arn", "--output", "text")
			arn := strings.TrimSpace(string(b))
			if err != nil || !strings.Contains(arn, ":assumed-role/mlp-db-mcp-"+role+"/") {
				t.Fatal("AWS profile did not assume the expected pilot role")
			}
			cmd := exec.CommandContext(t.Context(), binary, "--aws-url", endpoint)
			cmd.Env = env
			client := mcp.NewClient(&mcp.Implementation{Name: "personal-pilot-check", Version: "1"}, nil)
			session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: cmd}, nil)
			if err != nil {
				t.Fatal("could not start the signed MCP bridge")
			}
			t.Cleanup(func() { _ = session.Close() })
			identity := toolCall(t, session, "whoami", "{}", "ok")
			if identity["user"] != arn || identity["transport"] != "aws-iam" {
				t.Fatal("router identity differs from STS")
			}
			discovery := toolCall(t, session, "list_targets", "{}", "ok")
			targets, ok := discovery["targets"].([]any)
			if !ok || role == "reader" && len(targets) != 1 || role == "observer" && len(targets) != 0 {
				t.Fatal("incorrect discovery entitlement")
			}
			if role == "reader" {
				target, ok := targets[0].(map[string]any)
				if !ok || target["target"] != TargetName {
					t.Fatal("reader discovered the wrong target")
				}
			}
			readOutcome := "ok"
			switch {
			case role == "observer":
				readOutcome = "denied"
			case phase == "identity":
				readOutcome = "unavailable"
			}
			read := toolCall(t, session, "run_sql", countArgs, readOutcome)
			if role == "reader" && phase == "identity" && read["message"] != "The private database executor is unavailable." {
				t.Fatal("identity-stage reader did not report its unconfigured executor")
			}
			if readOutcome == "ok" {
				rows, _ := json.Marshal(read["rows"])
				if string(rows) != "[[32]]" {
					t.Fatal("private reader failed the known count")
				}
			}
			expected = append(expected,
				auditEvent{RequestID: identity["request_id"].(string), Caller: arn, Operation: "whoami", Outcome: "ok"},
				auditEvent{RequestID: discovery["request_id"].(string), Caller: arn, Operation: "list_targets", Outcome: "ok"},
				auditEvent{RequestID: read["request_id"].(string), Caller: arn, Operation: "run_sql", Outcome: readOutcome})
			t.Logf("audit request IDs: whoami=%s list_targets=%s run_sql=%s", identity["request_id"], discovery["request_id"], read["request_id"])
			names := []string{"mlp-db-mcp-router"}
			if phase == "hourly" {
				names = append(names, "mlp-db-mcp-executor")
			}
			for _, name := range names {
				out, err := cli("lambda", "invoke", "--function-name", name, "--invocation-type", "DryRun", filepath.Join(t.TempDir(), "result"))
				if err == nil || !bytes.Contains(out, []byte("AccessDeniedException")) {
					t.Fatalf("expected AWS to deny direct invocation of %s", name)
				}
			}
		})
	}
	if t.Failed() {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 75*time.Second)
	defer cancel()
	readLogs := func(ctx context.Context, group, pattern string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "aws", "logs", "filter-log-events", "--log-group-name", group,
			"--filter-pattern", pattern, "--query", "events[].message", "--output", "json")
		cmd.Env = liveProfileEnv(operator)
		return cmd.Output()
	}
	if err := waitForPilotAudit(ctx, expected, phase == "hourly", readLogs, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s CloudWatch audit receipts verified for both callers", phase)
}

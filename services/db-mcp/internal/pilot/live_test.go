//go:build awspilot

package pilot

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This opt-in test uses a deployed candidate. It does not provision resources.
func TestLivePilot(t *testing.T) {
	phase := os.Getenv("PILOT_PHASE")
	endpoint := os.Getenv("PILOT_URL")
	if os.Getenv("MLP_USE_REAL_AWS") != "1" || (phase != "identity" && phase != "hourly") ||
		!functionURL.MatchString(endpoint) {
		t.Fatal("set the explicit live opt-in, signed URL and phase")
	}
	binary, err := filepath.Abs("../../../../bin/mlp-db-mcp")
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"reader", "observer"} {
		t.Run(role, func(t *testing.T) {
			profile := os.Getenv("PILOT_" + strings.ToUpper(role) + "_PROFILE")
			if profile == "" {
				t.Fatal("supply an AWS CLI profile for this pilot role")
			}
			// The SDK and AWS CLI must resolve the same explicitly selected
			// shared profile. Remove ambient providers and endpoint overrides.
			env := []string{"HOME=" + os.Getenv("HOME"), "PATH=" + os.Getenv("PATH"), "AWS_PROFILE=" + profile,
				"AWS_REGION=us-east-1", "AWS_DEFAULT_REGION=us-east-1", "AWS_EC2_METADATA_DISABLED=true", "AWS_PAGER=", "MLP_USE_REAL_AWS=1"}
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
			identity := toolCall(t, session, "whoami", "{}")
			if identity["user"] != arn || identity["transport"] != "aws-iam" {
				t.Fatal("router identity differs from STS")
			}
			discovery := toolCall(t, session, "list_targets", "{}")
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
			read := toolCall(t, session, "run_sql", countArgs)
			rows, _ := json.Marshal(read["rows"])
			switch {
			case role == "observer":
				if read["code"] != "denied" {
					t.Fatal("known target was not denied to the observer")
				}
			case phase == "identity":
				if read["code"] != "unavailable" {
					t.Fatal("identity-only stage unexpectedly reached an executor")
				}
			default:
				if read["ok"] != true || string(rows) != "[[32]]" {
					t.Fatal("private reader failed the known count")
				}
			}
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
}

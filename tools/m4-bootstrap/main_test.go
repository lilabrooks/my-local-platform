package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	testRunID  = "20260909T200000Z"
	testCommit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testImage  = "123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/relay@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type commandCall struct {
	arguments []string
	stdin     []byte
}

type fakeRunner struct {
	calls  []commandCall
	handle func([]byte, []string) ([]byte, error)
}

func (f *fakeRunner) Run(_ context.Context, stdin []byte, arguments ...string) ([]byte, error) {
	f.calls = append(f.calls, commandCall{
		arguments: append([]string(nil), arguments...),
		stdin:     append([]byte(nil), stdin...),
	})
	return f.handle(stdin, arguments)
}

func jsonBytes(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func secretJSON(t *testing.T, value string) []byte {
	t.Helper()
	return jsonBytes(t, map[string]string{"SecretString": value})
}

func TestSigningSecretReusesExistingValue(t *testing.T) {
	existing := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
		if strings.Contains(strings.Join(arguments, " "), "put-secret-value") {
			t.Fatal("existing secret was replaced")
		}
		return secretJSON(t, existing), nil
	}}
	app := &application{runner: runner, random: bytes.NewReader(make([]byte, 32))}

	got, err := app.signingSecret(context.Background(), "arn:signing")
	if err != nil {
		t.Fatal(err)
	}
	if got != existing {
		t.Fatalf("signing value = %q", got)
	}
}

func TestSigningSecretIsGeneratedOnceAndSentThroughStdin(t *testing.T) {
	gets := 0
	runner := &fakeRunner{handle: func(stdin []byte, arguments []string) ([]byte, error) {
		joined := strings.Join(arguments, " ")
		if strings.Contains(joined, "get-secret-value") {
			gets++
			if gets == 1 {
				return nil, &commandError{arguments: arguments, stderr: "ResourceNotFoundException", err: errors.New("exit 254")}
			}
			return secretJSON(t, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), nil
		}
		if strings.Contains(joined, "put-secret-value") {
			if string(stdin) != "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" {
				t.Fatalf("put stdin = %q", stdin)
			}
			if strings.Contains(joined, string(stdin)) || !strings.Contains(joined, "file:///dev/stdin") {
				t.Fatalf("unsafe put arguments: %v", arguments)
			}
			return []byte(`{}`), nil
		}
		t.Fatalf("unexpected command: %v", arguments)
		return nil, nil
	}}
	app := &application{runner: runner, random: bytes.NewReader(make([]byte, 32))}

	got, err := app.signingSecret(context.Background(), "arn:signing")
	if err != nil {
		t.Fatal(err)
	}
	if got != "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" || gets != 2 {
		t.Fatalf("value = %q, gets = %d", got, gets)
	}
}

func TestSigningSecretPropagatesStorageFailure(t *testing.T) {
	want := errors.New("write denied")
	runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
		if strings.Contains(strings.Join(arguments, " "), "get-secret-value") {
			return nil, &commandError{arguments: arguments, stderr: "ResourceNotFoundException", err: errors.New("exit 254")}
		}
		return nil, want
	}}
	app := &application{runner: runner, random: bytes.NewReader(make([]byte, 32))}

	_, err := app.signingSecret(context.Background(), "arn:signing")
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestDatabaseURLQuotesTheManagedCredential(t *testing.T) {
	got, err := databaseURL("mlp-dev.abc.us-east-1.rds.amazonaws.com:5432", `{"username":"platform","password":"p@ss:/?# word"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got != "postgres://platform:p%40ss%3A%2F%3F%23%20word@mlp-dev.abc.us-east-1.rds.amazonaws.com:5432/platform?sslmode=verify-full" {
		t.Fatalf("database URL = %q", got)
	}
}

func TestBootstrapJobUsesThePinnedImageAndRestrictedPod(t *testing.T) {
	value, err := jobManifest(testRunID, testCommit, testImage)
	if err != nil {
		t.Fatal(err)
	}
	var job map[string]any
	if err := json.Unmarshal(value, &job); err != nil {
		t.Fatal(err)
	}
	spec := job["spec"].(map[string]any)
	if spec["backoffLimit"] != float64(0) || spec["activeDeadlineSeconds"] != float64(150) {
		t.Fatalf("job limits = %#v", spec)
	}
	pod := spec["template"].(map[string]any)["spec"].(map[string]any)
	if pod["serviceAccountName"] != "relay-bootstrap" || pod["restartPolicy"] != "Never" {
		t.Fatalf("pod identity = %#v", pod)
	}
	container := pod["containers"].([]any)[0].(map[string]any)
	if container["image"] != testImage || container["command"].([]any)[0] != "/relay-bootstrap" {
		t.Fatalf("container = %#v", container)
	}
	security := container["securityContext"].(map[string]any)
	if security["allowPrivilegeEscalation"] != false || security["readOnlyRootFilesystem"] != true {
		t.Fatalf("container security = %#v", security)
	}
}

func writeRunFiles(t *testing.T, root string, now time.Time, msk string) {
	t.Helper()
	raw := filepath.Join(root, ".evidence", "m4", testRunID)
	if err := os.MkdirAll(filepath.Join(raw, "rendered"), 0o700); err != nil {
		t.Fatal(err)
	}
	zero := 0
	files := map[string]any{
		"06-go-no-go.json": goPacket{
			SchemaVersion:   1,
			RunID:           testRunID,
			SourceCommit:    testCommit,
			Decision:        "go",
			ImageReferences: map[string]string{"relay": testImage},
			Gate: struct {
				Passed bool `json:"passed"`
			}{Passed: true},
		},
		"controller-state.json": controllerState{
			SchemaVersion: 1,
			RunID:         testRunID,
			Commit:        testCommit,
			Region:        "us-east-1",
			ControllerPID: 42,
			UpdatedAt:     now.UTC().Format(time.RFC3339),
			Phase:         "live",
			ApplyExit:     &zero,
		},
		"01-identity.txt": map[string]any{
			"aws": map[string]string{"account_id": "123456789012"},
		},
		"00-session.json": sessionReceipt{
			SchemaVersion:   1,
			RunID:           testRunID,
			Commit:          testCommit,
			Region:          "us-east-1",
			DestroyDeadline: now.Add(time.Hour).UTC().Format(time.RFC3339),
		},
		"rendered/relay-runtime.json": map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"data": map[string]string{
				"KAFKA_BOOTSTRAP": msk,
				"KAFKA_AUTH_MODE": "aws_msk_iam",
				"AWS_REGION":      "us-east-1",
			},
		},
	}
	for name, value := range files {
		if err := os.WriteFile(filepath.Join(raw, name), jsonBytes(t, value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func outputDocument(t *testing.T, values map[string]string) []byte {
	t.Helper()
	outputs := make(map[string]map[string]any, len(values))
	for key, value := range values {
		outputs[key] = map[string]any{"sensitive": false, "type": "string", "value": value}
	}
	return jsonBytes(t, outputs)
}

func TestRunBindsInputsAndKeepsSecretsOutOfArgumentsAndOutput(t *testing.T) {
	t.Setenv("MLP_USE_REAL_AWS", "1")
	t.Setenv("AWS_ENDPOINT_URL", "")
	now := time.Date(2026, 9, 9, 20, 1, 0, 0, time.UTC)
	root := t.TempDir()
	msk := "boot.example.kafka-serverless.us-east-1.amazonaws.com:9098"
	writeRunFiles(t, root, now, msk)
	outputs := outputDocument(t, map[string]string{
		"eks_cluster_name":        "mlp-dev",
		"msk_bootstrap_brokers":   msk,
		"rds_endpoint":            "mlp-dev.abc.us-east-1.rds.amazonaws.com:5432",
		"rds_master_secret_arn":   "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds-secret",
		"sink_signing_secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:signing-secret",
	})
	signing := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	password := "private-rds-password"
	var console bytes.Buffer
	runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
		joined := strings.Join(arguments, " ")
		switch {
		case joined == "aws sts get-caller-identity --query Account --output text":
			return []byte("123456789012\n"), nil
		case joined == "git rev-parse HEAD":
			return []byte(testCommit + "\n"), nil
		case joined == "git status --porcelain":
			return nil, nil
		case strings.HasPrefix(joined, "terraform "):
			return outputs, nil
		case strings.Contains(joined, "eks describe-cluster"):
			return []byte("https://cluster.example\n"), nil
		case joined == "kubectl config view --minify -o json":
			return []byte(`{"clusters":[{"cluster":{"server":"https://cluster.example"}}]}`), nil
		case strings.Contains(joined, "get-secret-value") && strings.Contains(joined, "secret:signing-secret"):
			return secretJSON(t, signing), nil
		case strings.Contains(joined, "get-secret-value") && strings.Contains(joined, "secret:rds-secret"):
			return secretJSON(t, `{"username":"platform","password":"`+password+`"}`), nil
		case joined == "kubectl create -f - -o name":
			return []byte("job.batch/relay-bootstrap-abc12\n"), nil
		case joined == "kubectl -n mlp logs job.batch/relay-bootstrap-abc12":
			return []byte("topic mlp.relay.deliveries partitions=12 ready\nrelay database ready active_subscriptions=19\nunsafe-test " + signing + " " + password + "\n"), nil
		case strings.HasPrefix(joined, "kubectl "):
			return nil, nil
		default:
			t.Fatalf("unexpected command: %s", joined)
			return nil, nil
		}
	}}
	app := &application{
		root:           root,
		runner:         runner,
		random:         bytes.NewReader(make([]byte, 32)),
		now:            func() time.Time { return now },
		processRunning: func(int) bool { return true },
		output:         &console,
	}

	if err := app.run(context.Background(), testRunID, testCommit, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	for _, sensitive := range []string{signing, password, "postgres://"} {
		if strings.Contains(console.String(), sensitive) {
			t.Fatalf("console contains sensitive value %q", sensitive)
		}
		for _, call := range runner.calls {
			if strings.Contains(strings.Join(call.arguments, " "), sensitive) {
				t.Fatalf("command arguments contain sensitive value %q: %v", sensitive, call.arguments)
			}
		}
	}
	if !strings.Contains(console.String(), "active_subscriptions=19") || !strings.Contains(console.String(), "bootstrap complete") {
		t.Fatalf("console = %q", console.String())
	}
	secretApplies := 0
	for _, call := range runner.calls {
		if strings.Join(call.arguments, " ") == "kubectl apply -f -" && bytes.Contains(call.stdin, []byte(signing)) {
			secretApplies++
		}
	}
	if secretApplies != 1 {
		t.Fatalf("secret apply count = %d", secretApplies)
	}
}

func TestClusterMismatchStopsBeforeSecretRetrieval(t *testing.T) {
	runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
		if arguments[0] == "aws" {
			return []byte("https://expected.example\n"), nil
		}
		return []byte(`{"clusters":[{"cluster":{"server":"https://other.example"}}]}`), nil
	}}
	app := &application{runner: runner}
	if err := app.verifyCluster(context.Background(), "mlp-dev"); err == nil || !strings.Contains(err.Error(), "does not target") {
		t.Fatalf("error = %v", err)
	}
}

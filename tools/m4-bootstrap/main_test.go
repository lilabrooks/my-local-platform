package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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
	deadline  time.Time
	hasLimit  bool
}

type fakeRunner struct {
	calls           []commandCall
	handle          func([]byte, []string) ([]byte, error)
	requireDeadline bool
}

func (f *fakeRunner) Run(ctx context.Context, stdin []byte, arguments ...string) ([]byte, error) {
	deadline, hasDeadline := ctx.Deadline()
	if f.requireDeadline {
		if !hasDeadline {
			return nil, errors.New("command context has no deadline")
		}
	}
	f.calls = append(f.calls, commandCall{
		arguments: append([]string(nil), arguments...),
		stdin:     append([]byte(nil), stdin...),
		deadline:  deadline,
		hasLimit:  hasDeadline,
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

	got, err := app.signingSecret(context.Background(), "arn:signing", nil)
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

	got, err := app.signingSecret(context.Background(), "arn:signing", nil)
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

	_, err := app.signingSecret(context.Background(), "arn:signing", nil)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestSigningSecretStopsBeforeWriteWhenControllerEnds(t *testing.T) {
	runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
		if strings.Contains(strings.Join(arguments, " "), "put-secret-value") {
			t.Fatal("secret was stored after the controller ended")
		}
		return nil, &commandError{arguments: arguments, stderr: "ResourceNotFoundException", err: errors.New("exit 254")}
	}}
	app := &application{runner: runner, random: bytes.NewReader(make([]byte, 32))}
	want := errors.New("controller ended")
	_, err := app.signingSecret(context.Background(), "arn:signing", func() error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestDatabaseURLQuotesTheManagedCredential(t *testing.T) {
	got, err := databaseURL("mlp-dev.abc.us-east-1.rds.amazonaws.com:5432", `{"username":"platform","password":"p@ss:/?# word"}`)
	if err != nil {
		t.Fatal(err)
	}
	if got != "postgres://platform:p%40ss%3A%2F%3F%23%20word@mlp-dev.abc.us-east-1.rds.amazonaws.com:5432/platform?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fcerts%2Frds-us-east-1-bundle.pem" {
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
	if spec["backoffLimit"] != float64(0) || spec["activeDeadlineSeconds"] != float64(jobActiveDeadline) {
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

func readRunFile[T any](t *testing.T, root, name string) T {
	t.Helper()
	var value T
	body, err := os.ReadFile(filepath.Join(root, ".evidence", "m4", testRunID, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func writeRunFile(t *testing.T, root, name string, value any) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, ".evidence", "m4", testRunID, name), jsonBytes(t, value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRunRejectsMalformedIdentifiers(t *testing.T) {
	t.Setenv("MLP_USE_REAL_AWS", "1")
	t.Setenv("AWS_ENDPOINT_URL", "")
	app := &application{root: t.TempDir()}
	tests := []struct {
		runID  string
		commit string
		region string
		want   string
	}{
		{testRunID, "short", "us-east-1", "full lowercase git SHA"},
		{testRunID, testCommit, "us-west-2", "fixed us-east-1 region"},
		{"invalid", testCommit, "us-east-1", "UTC YYYYMMDDTHHMMSSZ"},
	}
	for _, test := range tests {
		_, err := app.validateRun(context.Background(), test.runID, test.commit, test.region)
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("validateRun(%q, %q, %q) error = %v", test.runID, test.commit, test.region, err)
		}
	}
}

func TestValidateRunRejectsInvalidBindings(t *testing.T) {
	type inputs struct {
		stsAccount     string
		head           string
		status         string
		processRunning bool
	}
	tests := []struct {
		name   string
		mutate func(*testing.T, string, time.Time, *inputs)
		want   string
	}{
		{
			name: "real AWS opt-in missing",
			mutate: func(t *testing.T, _ string, _ time.Time, _ *inputs) {
				t.Setenv("MLP_USE_REAL_AWS", "")
			},
			want: "MLP_USE_REAL_AWS=1 is required",
		},
		{
			name: "emulator endpoint present",
			mutate: func(t *testing.T, _ string, _ time.Time, _ *inputs) {
				t.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
			},
			want: "AWS_ENDPOINT_URL must be unset",
		},
		{
			name: "unapproved relay image",
			mutate: func(t *testing.T, root string, _ time.Time, _ *inputs) {
				packet := readRunFile[goPacket](t, root, "06-go-no-go.json")
				packet.ImageReferences["relay"] = "relay:latest"
				writeRunFile(t, root, "06-go-no-go.json", packet)
			},
			want: "go/no-go packet does not approve",
		},
		{
			name: "account receipt mismatch",
			mutate: func(t *testing.T, root string, _ time.Time, _ *inputs) {
				receipt := readRunFile[accountReceipt](t, root, "01-identity.txt")
				receipt.AWS.AccountID = "210987654321"
				writeRunFile(t, root, "01-identity.txt", receipt)
			},
			want: "account receipt does not match",
		},
		{
			name: "current STS account mismatch",
			mutate: func(_ *testing.T, _ string, _ time.Time, values *inputs) {
				values.stsAccount = "210987654321"
			},
			want: "current AWS identity does not match",
		},
		{
			name: "stale controller heartbeat",
			mutate: func(t *testing.T, root string, now time.Time, _ *inputs) {
				state := readRunFile[controllerState](t, root, "controller-state.json")
				state.UpdatedAt = now.Add(-heartbeatMaxAge - time.Second).Format(time.RFC3339)
				writeRunFile(t, root, "controller-state.json", state)
			},
			want: "live controller is not active",
		},
		{
			name: "dead controller process",
			mutate: func(_ *testing.T, _ string, _ time.Time, values *inputs) {
				values.processRunning = false
			},
			want: "live controller is not active",
		},
		{
			name: "controller is destroying",
			mutate: func(t *testing.T, root string, _ time.Time, _ *inputs) {
				state := readRunFile[controllerState](t, root, "controller-state.json")
				state.Phase = "destroying"
				writeRunFile(t, root, "controller-state.json", state)
			},
			want: "live controller is not active",
		},
		{
			name: "terraform apply failed",
			mutate: func(t *testing.T, root string, _ time.Time, _ *inputs) {
				state := readRunFile[controllerState](t, root, "controller-state.json")
				one := 1
				state.ApplyExit = &one
				writeRunFile(t, root, "controller-state.json", state)
			},
			want: "live controller is not active",
		},
		{
			name: "destroy deadline expired",
			mutate: func(t *testing.T, root string, now time.Time, _ *inputs) {
				session := readRunFile[sessionReceipt](t, root, "00-session.json")
				session.DestroyDeadline = now.Add(-time.Second).Format(time.RFC3339)
				writeRunFile(t, root, "00-session.json", session)
			},
			want: "session receipt is not active",
		},
		{
			name: "git HEAD mismatch",
			mutate: func(_ *testing.T, _ string, _ time.Time, values *inputs) {
				values.head = strings.Repeat("b", 40)
			},
			want: "worktree HEAD does not match",
		},
		{
			name: "dirty worktree",
			mutate: func(_ *testing.T, _ string, _ time.Time, values *inputs) {
				values.status = " M README.md\n"
			},
			want: "worktree must be clean",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("MLP_USE_REAL_AWS", "1")
			t.Setenv("AWS_ENDPOINT_URL", "")
			now := time.Now().UTC().Truncate(time.Second)
			root := t.TempDir()
			writeRunFiles(t, root, now, "broker:9098")
			values := inputs{
				stsAccount:     "123456789012",
				head:           testCommit,
				processRunning: true,
			}
			test.mutate(t, root, now, &values)
			runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
				switch strings.Join(arguments, " ") {
				case "aws sts get-caller-identity --query Account --output text":
					return []byte(values.stsAccount + "\n"), nil
				case "git rev-parse HEAD":
					return []byte(values.head + "\n"), nil
				case "git status --porcelain":
					return []byte(values.status), nil
				default:
					t.Fatalf("unexpected command: %v", arguments)
					return nil, nil
				}
			}}
			app := &application{
				root:           root,
				runner:         runner,
				now:            func() time.Time { return now },
				processRunning: func(int) bool { return values.processRunning },
			}
			_, err := app.validateRun(context.Background(), testRunID, testCommit, "us-east-1")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
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

func TestRunRejectsTerraformBindingMismatch(t *testing.T) {
	tests := []struct {
		name       string
		fileMSK    string
		outputMSK  string
		rdsARN     string
		signingARN string
		want       string
	}{
		{
			name:       "secret ARN account",
			fileMSK:    "broker:9098",
			outputMSK:  "broker:9098",
			rdsARN:     "arn:aws:secretsmanager:us-east-1:210987654321:secret:rds",
			signingARN: "arn:aws:secretsmanager:us-east-1:123456789012:secret:signing",
			want:       "secret ARNs do not match",
		},
		{
			name:       "rendered MSK brokers",
			fileMSK:    "rendered:9098",
			outputMSK:  "terraform:9098",
			rdsARN:     "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds",
			signingARN: "arn:aws:secretsmanager:us-east-1:123456789012:secret:signing",
			want:       "runtime does not match the Terraform MSK brokers",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("MLP_USE_REAL_AWS", "1")
			t.Setenv("AWS_ENDPOINT_URL", "")
			now := time.Now().UTC().Truncate(time.Second)
			root := t.TempDir()
			writeRunFiles(t, root, now, test.fileMSK)
			outputs := outputDocument(t, map[string]string{
				"eks_cluster_name":        "mlp-dev",
				"msk_bootstrap_brokers":   test.outputMSK,
				"rds_endpoint":            "mlp-dev.abc.us-east-1.rds.amazonaws.com:5432",
				"rds_master_secret_arn":   test.rdsARN,
				"sink_signing_secret_arn": test.signingARN,
			})
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
				default:
					t.Fatalf("unexpected command before binding rejection: %s", joined)
					return nil, nil
				}
			}}
			app := &application{
				root:           root,
				runner:         runner,
				now:            func() time.Time { return now },
				processRunning: func(int) bool { return true },
			}
			ctx, cancel := context.WithTimeout(context.Background(), operatorTimeout)
			defer cancel()
			err := app.run(ctx, testRunID, testCommit, "us-east-1")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestRunBindsInputsAndKeepsSecretsOutOfArgumentsAndOutput(t *testing.T) {
	t.Setenv("MLP_USE_REAL_AWS", "1")
	t.Setenv("AWS_ENDPOINT_URL", "")
	now := time.Now().UTC().Truncate(time.Second)
	root := t.TempDir()
	msk := "boot.example.kafka-serverless.us-east-1.amazonaws.com:9098"
	writeRunFiles(t, root, now, msk)
	session := readRunFile[sessionReceipt](t, root, "00-session.json")
	sessionDeadline := now.Add(time.Minute)
	session.DestroyDeadline = sessionDeadline.Format(time.RFC3339)
	writeRunFile(t, root, "00-session.json", session)
	outputs := outputDocument(t, map[string]string{
		"eks_cluster_name":        "mlp-dev",
		"msk_bootstrap_brokers":   msk,
		"rds_endpoint":            "mlp-dev.abc.us-east-1.rds.amazonaws.com:5432",
		"rds_master_secret_arn":   "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds-secret",
		"sink_signing_secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:signing-secret",
	})
	signing := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	password := "p@ss word"
	var console bytes.Buffer
	runner := &fakeRunner{requireDeadline: true, handle: func(_ []byte, arguments []string) ([]byte, error) {
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
		case joined == "kubectl -n mlp get job.batch/relay-bootstrap-abc12 -o json":
			return []byte(`{"status":{"conditions":[{"type":"Complete","status":"True"}]}}`), nil
		case joined == "kubectl -n mlp logs job.batch/relay-bootstrap-abc12":
			return []byte("topic mlp.relay.deliveries partitions=12 ready\nrelay database ready active_subscriptions=19\nunsafe-test " + signing + " " + password + " " + encodedPassword(password) + "\n"), nil
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

	ctx, cancel := context.WithTimeout(context.Background(), operatorTimeout)
	defer cancel()
	if err := app.run(ctx, testRunID, testCommit, "us-east-1"); err != nil {
		t.Fatal(err)
	}
	for _, sensitive := range []string{signing, password, encodedPassword(password), "postgres://"} {
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
		if strings.Join(call.arguments, " ") == "kubectl apply --server-side --field-manager=mlp-bootstrap -f -" && bytes.Contains(call.stdin, []byte(signing)) {
			secretApplies++
		}
	}
	if secretApplies != 1 {
		t.Fatalf("secret apply count = %d", secretApplies)
	}
	validated := false
	for _, call := range runner.calls {
		if len(call.arguments) > 0 && call.arguments[0] == "terraform" {
			validated = true
		}
		if validated && (!call.hasLimit || call.deadline.After(sessionDeadline)) {
			t.Fatalf("post-validation command %v has deadline %v, session deadline %v", call.arguments, call.deadline, sessionDeadline)
		}
	}
	if !validated {
		t.Fatal("terraform output was never read")
	}
}

func TestRunStopsBeforeJobWhenControllerStartsDestroying(t *testing.T) {
	t.Setenv("MLP_USE_REAL_AWS", "1")
	t.Setenv("AWS_ENDPOINT_URL", "")
	now := time.Now().UTC().Truncate(time.Second)
	root := t.TempDir()
	msk := "broker:9098"
	writeRunFiles(t, root, now, msk)
	outputs := outputDocument(t, map[string]string{
		"eks_cluster_name":        "mlp-dev",
		"msk_bootstrap_brokers":   msk,
		"rds_endpoint":            "mlp-dev.abc.us-east-1.rds.amazonaws.com:5432",
		"rds_master_secret_arn":   "arn:aws:secretsmanager:us-east-1:123456789012:secret:rds",
		"sink_signing_secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:signing",
	})
	signing := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	jobCreated := false
	runner := &fakeRunner{handle: func(stdin []byte, arguments []string) ([]byte, error) {
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
		case strings.Contains(joined, "get-secret-value") && strings.Contains(joined, "secret:signing"):
			return secretJSON(t, signing), nil
		case strings.Contains(joined, "get-secret-value") && strings.Contains(joined, "secret:rds"):
			return secretJSON(t, `{"username":"platform","password":"private"}`), nil
		case joined == "kubectl apply --server-side --field-manager=mlp-bootstrap -f -" && bytes.Contains(stdin, []byte(signing)):
			state := readRunFile[controllerState](t, root, "controller-state.json")
			state.Phase = "destroying"
			writeRunFile(t, root, "controller-state.json", state)
			return nil, nil
		case joined == "kubectl create -f - -o name":
			jobCreated = true
			return nil, errors.New("Job must not be created")
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
		output:         io.Discard,
	}
	ctx, cancel := context.WithTimeout(context.Background(), operatorTimeout)
	defer cancel()
	err := app.run(ctx, testRunID, testCommit, "us-east-1")
	if err == nil || !strings.Contains(err.Error(), "live controller is not active") {
		t.Fatalf("error = %v", err)
	}
	if jobCreated {
		t.Fatal("bootstrap Job was created after the controller began destroying")
	}
}

func TestWaitForJobReturnsFailedConditionImmediately(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	root := t.TempDir()
	writeRunFiles(t, root, now, "broker:9098")
	runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
		if strings.Join(arguments, " ") != "kubectl -n mlp get job.batch/relay-bootstrap-failed -o json" {
			t.Fatalf("unexpected command: %v", arguments)
		}
		return []byte(`{"status":{"conditions":[{"type":"Failed","status":"True","reason":"BackoffLimitExceeded","message":"container exited 1"}]}}`), nil
	}}
	app := &application{
		root:           root,
		runner:         runner,
		now:            func() time.Time { return now },
		processRunning: func(int) bool { return true },
	}
	err := app.waitForJob(context.Background(), "job.batch/relay-bootstrap-failed", testRunID, testCommit, "us-east-1")
	if err == nil || !strings.Contains(err.Error(), "BackoffLimitExceeded: container exited 1") {
		t.Fatalf("error = %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("status calls = %d, want 1", len(runner.calls))
	}
}

func TestWaitForJobStopsWhenControllerStartsDestroying(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	root := t.TempDir()
	writeRunFiles(t, root, now, "broker:9098")
	statusReads := 0
	runner := &fakeRunner{handle: func(_ []byte, arguments []string) ([]byte, error) {
		if strings.Join(arguments, " ") != "kubectl -n mlp get job.batch/relay-bootstrap-pending -o json" {
			t.Fatalf("unexpected command: %v", arguments)
		}
		statusReads++
		state := readRunFile[controllerState](t, root, "controller-state.json")
		state.Phase = "destroying"
		writeRunFile(t, root, "controller-state.json", state)
		return []byte(`{"status":{}}`), nil
	}}
	ready := make(chan time.Time)
	close(ready)
	app := &application{
		root:           root,
		runner:         runner,
		now:            func() time.Time { return now },
		processRunning: func(int) bool { return true },
		after:          func(time.Duration) <-chan time.Time { return ready },
	}
	err := app.waitForJob(context.Background(), "job.batch/relay-bootstrap-pending", testRunID, testCommit, "us-east-1")
	if err == nil || !strings.Contains(err.Error(), "live controller stopped during runtime bootstrap") {
		t.Fatalf("error = %v", err)
	}
	if statusReads != 1 {
		t.Fatalf("status reads = %d, want 1", statusReads)
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

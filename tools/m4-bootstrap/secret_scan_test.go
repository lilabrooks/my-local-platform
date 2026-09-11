package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSecretScanRejectsConcurrentBootstrapAndRetainsPartialCoverage(t *testing.T) {
	root := t.TempDir()
	writeRunFiles(t, root, time.Now().UTC(), "broker:9098")
	scan, err := openSecretScan(root, testRunID, testCommit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openSecretScan(root, testRunID, testCommit); err == nil {
		t.Fatal("second bootstrap acquired the same run")
	}
	if err := scan.begin(); err != nil {
		t.Fatal(err)
	}
	if err := scan.record("SIGNING_SECRET", "synthetic-signing-value-only"); err != nil {
		t.Fatal(err)
	}
	partial := readRunFile[secretScan](t, root, "secret-scan.json")
	if partial.State != "collecting" || len(partial.Entries) != 7 {
		t.Fatalf("partial coverage = %+v", partial)
	}
	if err := scan.complete(); err == nil {
		t.Fatal("partial scan completed")
	}
}

func TestSigningFingerprintFailurePreventsSecretMutation(t *testing.T) {
	runner := &fakeRunner{handle: func(_ []byte, args []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, " "), "put-secret-value") {
			t.Fatal("secret mutated without a durable fingerprint")
		}
		return nil, &commandError{arguments: args, stderr: "ResourceNotFoundException", err: errors.New("exit 254")}
	}}
	app := &application{runner: runner, random: bytes.NewReader(make([]byte, 32))}
	want := errors.New("receipt write denied")
	_, err := app.signingSecret(context.Background(), "arn:test", nil, func(string) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandFailureDoesNotEchoSensitiveDiagnostics(t *testing.T) {
	err := &commandError{arguments: []string{"kubectl", "apply"}, stderr: "synthetic-secret-canary", err: errors.New("exit 1")}
	if strings.Contains(err.Error(), "canary") {
		t.Fatal("stderr escaped")
	}
}

func TestBootstrapLogsWithholdArbitraryNestedDiagnostics(t *testing.T) {
	canary := "synthetic quote\" slash\\ secret"
	inner, _ := json.Marshal(map[string]string{"secret": canary})
	outer, _ := json.Marshal(map[string]string{"message": string(inner)})
	logs := append([]byte("topic mlp.relay.deliveries partitions=12 ready\n"), outer...)
	got := string(redacted(logs, canary))
	if got != "topic mlp.relay.deliveries partitions=12 ready\n" {
		t.Fatalf("unexpected diagnostics: %q", got)
	}
}

func TestInitialScanJSONRejectsDuplicateUnknownAndNullEntries(t *testing.T) {
	root := t.TempDir()
	writeRunFiles(t, root, time.Now().UTC(), "broker:9098")
	path := filepath.Join(root, ".evidence", "m4", testRunID, "secret-scan.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !validInitialScanJSON(data) {
		t.Fatal("valid receipt rejected")
	}
	duplicate := append([]byte(`{"state":"not_started",`), data[1:]...)
	if validInitialScanJSON(duplicate) {
		t.Fatal("duplicate accepted")
	}
	for _, change := range []func(map[string]any){
		func(v map[string]any) { v["credential"] = "synthetic-secret-canary" },
		func(v map[string]any) { v["schema_version"] = true },
		func(v map[string]any) { v["entries"] = nil },
	} {
		t.Run("malformed", func(t *testing.T) {
			fixture := t.TempDir()
			writeRunFiles(t, fixture, time.Now().UTC(), "broker:9098")
			value := readRunFile[map[string]any](t, fixture, "secret-scan.json")
			change(value)
			writeRunFile(t, fixture, "secret-scan.json", value)
			if _, err := openSecretScan(fixture, testRunID, testCommit); err == nil {
				t.Fatal("invalid scan accepted")
			}
		})
	}
}

func TestGoFingerprintsAreConsumedByPythonWithoutCredentials(t *testing.T) {
	root := t.TempDir()
	writeRunFiles(t, root, time.Now().UTC(), "broker:9098")
	writeRunFile(t, root, "00-preflight.json", map[string]any{"schema_version": 1, "run_id": testRunID, "commit": testCommit, "result": "passed"})
	scan, err := openSecretScan(root, testRunID, testCommit)
	if err != nil {
		t.Fatal(err)
	}
	if err = scan.begin(); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"SIGNING_SECRET":    "synthetic-signing-value-only",
		"DATABASE_PASSWORD": "synthetic quote\" slash\\ <&> 😀\x7f",
		"DATABASE_URL":      "postgres://platform:p%40ssword-long-value@db.invalid/platform?sslmode=verify-full",
	}
	for name, value := range values {
		if err = scan.record(name, value); err != nil {
			t.Fatal(err)
		}
	}
	if err = scan.complete(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(scan.path)
	for _, value := range values {
		if bytes.Contains(data, []byte(value)) {
			t.Fatal("receipt retained plaintext")
		}
	}
	script, err := filepath.Abs("../../scripts/m4-evidence.py")
	if err != nil {
		t.Fatal(err)
	}
	// The publisher receives only the Go receipt. Canary strings exist solely in
	// this test process and are sent to the verifier to exercise rejection.
	program := `import sys,json,importlib.util,pathlib,base64,urllib.parse
spec=importlib.util.spec_from_file_location("evidence",sys.argv[1]); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
raw=pathlib.Path(sys.argv[2]); scan=m.load_secret_scan(raw,sys.argv[3]); values=json.load(sys.stdin)
m.scan_secret_bytes(b"ordinary evidence after simulated secret deletion",scan,"proof")
for value in values.values():
    forms=[value,json.dumps(value)[1:-1],urllib.parse.quote_plus(value),base64.b64encode(value.encode()).decode(),base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")]
    for form in forms:
        for blob in [form,json.dumps({"logs":json.dumps({"credential":form})})]:
            try: m.scan_secret_bytes(blob.encode(),scan,"canary")
            except m.EvidenceError: pass
            else: raise AssertionError("encoded canary escaped")
`
	input, _ := json.Marshal(values)
	command := exec.Command("python3", "-c", program, script, filepath.Dir(scan.path), testRunID)
	command.Stdin = bytes.NewReader(input)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Python handoff: %v\n%s", err, output)
	}
}

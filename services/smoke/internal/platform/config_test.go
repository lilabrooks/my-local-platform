package platform

import (
	"os"
	"testing"
)

func TestLoadUsesDefaultsWhenUnset(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("KAFKA_BOOTSTRAP", "")

	cfg := Load()

	if cfg.AWSEndpoint != "http://localhost:4566" {
		t.Errorf("AWSEndpoint = %q, want the local floci endpoint", cfg.AWSEndpoint)
	}
	if cfg.KafkaBrokers != "localhost:9092" {
		t.Errorf("KafkaBrokers = %q, want localhost:9092", cfg.KafkaBrokers)
	}
	if cfg.RelayTopic != "mlp.relay.deliveries" {
		t.Errorf("RelayTopic = %q, want mlp.relay.deliveries", cfg.RelayTopic)
	}
}

func TestLoadPrefersEnvironment(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "http://floci:4566")
	t.Setenv("MLP_BUCKET", "other-bucket")

	cfg := Load()

	if cfg.AWSEndpoint != "http://floci:4566" {
		t.Errorf("AWSEndpoint = %q, want the value from the environment", cfg.AWSEndpoint)
	}
	if cfg.Bucket != "other-bucket" {
		t.Errorf("Bucket = %q, want other-bucket", cfg.Bucket)
	}
}

func TestEmptyRelayURLDisablesRelayCheck(t *testing.T) {
	t.Setenv("RELAY_INGEST_URL", "")

	if got := Load().RelayIngestURL; got != "" {
		t.Errorf("RelayIngestURL = %q, want an explicit empty value to be preserved", got)
	}
}

func TestUnsetRelayURLUsesLocalDefault(t *testing.T) {
	old, wasSet := os.LookupEnv("RELAY_INGEST_URL")
	if err := os.Unsetenv("RELAY_INGEST_URL"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if wasSet {
			if err := os.Setenv("RELAY_INGEST_URL", old); err != nil {
				t.Errorf("restore RELAY_INGEST_URL: %v", err)
			}
			return
		}
		if err := os.Unsetenv("RELAY_INGEST_URL"); err != nil {
			t.Errorf("clear RELAY_INGEST_URL: %v", err)
		}
	})

	if got := Load().RelayIngestURL; got != "http://localhost:8082" {
		t.Errorf("RelayIngestURL = %q, want the local default", got)
	}
}

// An empty AWS_ENDPOINT_URL must fall back to the local default rather than
// being treated as "use real AWS" -- otherwise `export AWS_ENDPOINT_URL=` in a
// shell would silently point the smoke checks at a real account.
func TestEmptyEndpointDoesNotMeanRealAWS(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "")

	if cfg := Load(); cfg.UsingRealAWS() {
		t.Error("an empty AWS_ENDPOINT_URL was treated as real AWS")
	}
}

func TestUsingRealAWS(t *testing.T) {
	if (Config{AWSEndpoint: ""}).UsingRealAWS() != true {
		t.Error("an unset endpoint should mean real AWS")
	}
	if (Config{AWSEndpoint: "http://localhost:4566"}).UsingRealAWS() != false {
		t.Error("a local endpoint should not mean real AWS")
	}
}

// Real AWS must require one explicit opt-in, and nothing else may reach it.
func TestRealAWSRequiresExplicitOptIn(t *testing.T) {
	t.Setenv("MLP_USE_REAL_AWS", "1")

	cfg := Load()

	if !cfg.UsingRealAWS() {
		t.Error("MLP_USE_REAL_AWS=1 should target real AWS")
	}
	if cfg.AWSEndpoint != "" {
		t.Errorf("AWSEndpoint = %q, want empty so the SDK resolves real endpoints", cfg.AWSEndpoint)
	}
}

// The opt-in beats an explicitly set local endpoint, so the intent is never
// ambiguous when both are present.
func TestOptInOverridesEndpoint(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "http://localhost:4566")
	t.Setenv("MLP_USE_REAL_AWS", "1")

	if cfg := Load(); !cfg.UsingRealAWS() {
		t.Error("MLP_USE_REAL_AWS=1 should win over a local endpoint")
	}
}

// Any other value is not an opt-in. "true", "yes" and "0" all stay local.
func TestOnlyExactlyOneOptsIn(t *testing.T) {
	for _, v := range []string{"", "0", "true", "yes", "TRUE"} {
		t.Setenv("MLP_USE_REAL_AWS", v)
		if cfg := Load(); cfg.UsingRealAWS() {
			t.Errorf("MLP_USE_REAL_AWS=%q should not target real AWS", v)
		}
	}
}

// The Tempo assertion is off unless asked for. CI's main smoke job and the
// documented `make up-apps` path both run without the obs profile, and a check
// that fails for a missing dependency rather than a missing trace would break
// both. `make smoke-traces` is what turns it on.
func TestRequireTracesIsOffUnlessExplicitlyEnabled(t *testing.T) {
	for _, tc := range []struct {
		value string
		set   bool
		want  bool
	}{
		{set: false, want: false},
		{value: "", set: true, want: false},
		{value: "0", set: true, want: false},
		{value: "true", set: true, want: false},
		{value: "1", set: true, want: true},
	} {
		name := "unset"
		if tc.set {
			name = "MLP_SMOKE_REQUIRE_TRACES=" + tc.value
			t.Setenv("MLP_SMOKE_REQUIRE_TRACES", tc.value)
		} else {
			os.Unsetenv("MLP_SMOKE_REQUIRE_TRACES")
		}
		if got := Load().RequireTraces; got != tc.want {
			t.Errorf("%s: RequireTraces = %v, want %v", name, got, tc.want)
		}
	}
}

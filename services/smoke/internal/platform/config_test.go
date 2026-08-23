package platform

import "testing"

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

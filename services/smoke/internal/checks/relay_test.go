package checks

import (
	"context"
	"strings"
	"testing"

	"github.com/lilabrooks/my-local-platform/smoke/internal/platform"
)

func TestRelayEmptyURLIsAnExplicitSkip(t *testing.T) {
	t.Parallel()

	detail, err := Relay(platform.Config{RelayIngestURL: ""}).Run(context.Background())
	if err != nil {
		t.Fatalf("Relay returned an error for a disabled check: %v", err)
	}
	if detail != "skipped (apps profile disabled)" {
		t.Fatalf("detail = %q, want an explicit skip", detail)
	}
}

func TestCheckAttemptHistory(t *testing.T) {
	t.Parallel()

	valid := []deliveryAttempt{
		{SubscriptionID: 1, SubscriptionURL: "http://sink:8081/hooks/ok", AttemptNumber: 1, Outcome: "delivered"},
		{SubscriptionID: 2, SubscriptionURL: "http://sink:8081/hooks/flaky", AttemptNumber: 1, Outcome: "retrying"},
		{SubscriptionID: 2, SubscriptionURL: "http://sink:8081/hooks/flaky", AttemptNumber: 2, Outcome: "retrying"},
		{SubscriptionID: 2, SubscriptionURL: "http://sink:8081/hooks/flaky", AttemptNumber: 3, Outcome: "exhausted"},
	}
	if err := checkAttemptHistory("evt_test", valid); err != nil {
		t.Fatalf("valid history: %v", err)
	}

	badOrder := append([]deliveryAttempt(nil), valid...)
	badOrder[3].AttemptNumber = 2
	if err := checkAttemptHistory("evt_test", badOrder); err == nil || !strings.Contains(err.Error(), "follows") {
		t.Errorf("duplicate attempt number error = %v, want ordering failure", err)
	}

	if err := checkAttemptHistory("evt_test", valid[:1]); err == nil {
		t.Error("history without the exhausted subscriber was accepted")
	}
	if err := checkAttemptHistory("evt_test", valid[:3]); err == nil || !strings.Contains(err.Error(), "exactly 4") {
		t.Errorf("short history error = %v, want exact-count failure", err)
	}
}

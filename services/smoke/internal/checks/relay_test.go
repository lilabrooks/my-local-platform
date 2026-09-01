package checks

import (
	"context"
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

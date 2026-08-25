package subscriptions

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestValidateURL(t *testing.T) {
	t.Parallel()

	ok := []string{
		"http://sink:8081/hooks/ok",
		"https://example.test/webhooks",
		"https://example.test:8443/a/b?c=d",
	}
	for _, raw := range ok {
		if err := ValidateURL(raw); err != nil {
			t.Errorf("ValidateURL(%q) = %v, want nil", raw, err)
		}
	}

	bad := map[string]string{
		"empty":         "",
		"no scheme":     "sink:8081/hooks",
		"file scheme":   "file:///etc/passwd",
		"ftp scheme":    "ftp://example.test/x",
		"scheme only":   "https://",
		"not a url":     "://://",
		"relative path": "/hooks/ok",
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateURL(raw); err == nil {
				t.Errorf("ValidateURL(%q) = nil, want an error", raw)
			}
		})
	}
}

// The signing secret must not reach a log line or an error message.
func TestSubscriptionStringRedactsSecret(t *testing.T) {
	t.Parallel()

	s := Subscription{ID: 7, TenantID: "acme", URL: "http://sink:8081/hooks/ok", Secret: "topsecretvalue"}
	got := s.String()
	if strings.Contains(got, "topsecretvalue") {
		t.Errorf("String() leaked the signing secret: %q", got)
	}
	for _, want := range []string{"7", "acme", "sink:8081"} {
		if !strings.Contains(got, want) {
			t.Errorf("String() = %q, want it to mention %q", got, want)
		}
	}
}

func TestZeroStoreDoesNotPanic(t *testing.T) {
	t.Parallel()

	var s *Store
	if _, err := s.ForTenant(context.Background(), "acme"); !errors.Is(err, ErrNoPool) {
		t.Errorf("nil store error = %v, want ErrNoPool", err)
	}
	if _, err := (&Store{}).ForTenant(context.Background(), "acme"); !errors.Is(err, ErrNoPool) {
		t.Errorf("empty store error = %v, want ErrNoPool", err)
	}
}

// Integration. Skips when the local stack is not up, so `make test` works
// without Postgres running; CI brings the stack up before running it.
func TestForTenantAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://platform:platform@localhost:5432/platform?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("no postgres pool (%v); run `make up` and `local/bootstrap/relay-db.sh`", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("postgres unreachable (%v); run `make up` and `local/bootstrap/relay-db.sh`", err)
	}

	store := New(pool)

	subs, err := store.ForTenant(ctx, "acme")
	if err != nil {
		t.Fatalf("ForTenant(acme): %v -- has local/bootstrap/relay-db.sh been run?", err)
	}
	// The seed gives acme two subscribers on purpose, so the partial-failure
	// path in the delivery consumer is exercisable.
	if len(subs) < 2 {
		t.Fatalf("acme has %d active subscriptions, want at least 2 (see local/bootstrap/relay-db.sh)", len(subs))
	}
	for _, s := range subs {
		if s.TenantID != "acme" {
			t.Errorf("got a subscription for tenant %q in acme's results", s.TenantID)
		}
		if s.Secret == "" {
			t.Errorf("%s has an empty signing secret", s)
		}
	}
	// Ordering must be stable, not planner-dependent.
	for i := 1; i < len(subs); i++ {
		if subs[i-1].ID >= subs[i].ID {
			t.Errorf("subscriptions not ordered by id: %d then %d", subs[i-1].ID, subs[i].ID)
		}
	}

	// An unknown tenant is empty, not an error: an event nobody subscribes to
	// is delivered nowhere, successfully.
	none, err := store.ForTenant(ctx, "nobody-by-this-name")
	if err != nil {
		t.Errorf("ForTenant(unknown) = %v, want no error", err)
	}
	if len(none) != 0 {
		t.Errorf("ForTenant(unknown) returned %d subscriptions, want 0", len(none))
	}

	if _, err := store.ForTenant(ctx, "  "); err == nil {
		t.Error("ForTenant(blank) succeeded, want an error")
	}
}

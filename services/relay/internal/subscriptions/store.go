// Package subscriptions reads the endpoints a tenant's events are delivered to.
//
// Read-only by design. Subscriptions are configuration in M1, seeded by
// local/bootstrap/relay-db.sh; nothing here writes them at runtime.
package subscriptions

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Subscription is one endpoint belonging to one tenant.
type Subscription struct {
	ID       int64
	TenantID string
	URL      string
	// Secret signs the delivery. It is never logged, which is why this type
	// has a String method that omits it.
	Secret string
}

// String redacts the signing secret. Subscriptions end up in log lines and
// error messages, and a secret that leaks once is leaked.
func (s Subscription) String() string {
	return fmt.Sprintf("subscription %d (tenant %s -> %s)", s.ID, s.TenantID, s.URL)
}

// Store answers "where do this tenant's events go".
type Store struct {
	pool *pgxpool.Pool
}

// New wraps an existing pool. The caller owns the pool's lifetime.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// ErrNoPool guards against a zero-value Store reaching a query.
var ErrNoPool = errors.New("subscriptions: no database pool")

// ForTenant returns the tenant's active subscriptions, oldest first so delivery
// order is stable across calls rather than whatever the planner returns.
//
// An unknown tenant is not an error: it returns no subscriptions. An event for
// a tenant nobody subscribes to is delivered nowhere, successfully.
func (s *Store) ForTenant(ctx context.Context, tenantID string) ([]Subscription, error) {
	if s == nil || s.pool == nil {
		return nil, ErrNoPool
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, errors.New("subscriptions: empty tenant id")
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, tenant_id, url, signing_secret
		FROM relay_subscriptions
		WHERE tenant_id = $1 AND active
		ORDER BY id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query subscriptions for %q: %w", tenantID, err)
	}
	defer rows.Close()

	var out []Subscription
	for rows.Next() {
		var sub Subscription
		if err := rows.Scan(&sub.ID, &sub.TenantID, &sub.URL, &sub.Secret); err != nil {
			return nil, fmt.Errorf("scan subscription: %w", err)
		}
		if err := ValidateURL(sub.URL); err != nil {
			// Refuse to deliver rather than POST somewhere unintended. A bad
			// row is an operator error and should be loud.
			return nil, fmt.Errorf("%s: %w", sub, err)
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read subscriptions for %q: %w", tenantID, err)
	}
	return out, nil
}

// ValidateURL rejects anything relay should not POST to. Subscriptions are
// operator-supplied, and a delivery target is a request this service makes on
// someone else's behalf.
func ValidateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("unparseable url %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("url %q has scheme %q, want http or https", raw, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("url %q has no host", raw)
	}
	return nil
}

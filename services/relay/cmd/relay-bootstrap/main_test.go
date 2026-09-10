package main

import (
	"errors"
	"strings"
	"testing"
)

func TestSafeErrorRemovesDatabaseAndSigningCredentials(t *testing.T) {
	database := "postgres://platform:p%40ssword@db.internal:5432/platform?sslmode=verify-full"
	t.Setenv("DATABASE_URL", database)
	t.Setenv("RELAY_SIGNING_SECRET", "private-signing-value")

	got := safeError(errors.New("failed " + database + " password=p@ssword signing=private-signing-value"))
	for _, secret := range []string{database, "p@ssword", "private-signing-value"} {
		if strings.Contains(got, secret) {
			t.Fatalf("safe error contains %q: %s", secret, got)
		}
	}
}

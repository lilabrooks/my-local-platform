//go:build unix

package fixture

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestLifecyclePreservesCredentialsUntilSuccessfulTeardown(t *testing.T) {
	root := t.TempDir()
	docker := filepath.Join(root, "docker")
	if err := os.WriteFile(docker, []byte("#!/bin/sh\n[ -n \"$METER_PG_READER\" ] && [ ! -f \"$DEMO_TEST_FAILURE\" ]\n"), 0700); err != nil {
		t.Fatal(err)
	}
	failure := filepath.Join(root, "fail")
	t.Setenv("PATH", root)
	t.Setenv("DEMO_TEST_FAILURE", failure)
	run := func(action string) error { return Run(root, action, io.Discard, io.Discard) }
	done := make(chan error, 2)
	for range 2 {
		go func() { done <- run("up") }()
	}
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	state := filepath.Join(root, "local", "tmp", "db-mcp")
	previous := map[string][]byte{}
	for _, user := range users {
		p := filepath.Join(state, user)
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(p)
		if err != nil || st.Mode().Perm() != 0600 || len(data) != 48 {
			t.Fatalf("invalid credential file for %s (%v)", user, err)
		}
		for _, other := range previous {
			if bytes.Equal(data, other) {
				t.Fatal("credentials reused")
			}
		}
		previous[user] = data
	}
	if err := os.WriteFile(failure, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run("down"); err == nil {
		t.Fatal("teardown unexpectedly succeeded")
	}
	for name, want := range previous {
		data, err := os.ReadFile(filepath.Join(state, name))
		if err != nil || !bytes.Equal(data, want) {
			t.Fatalf("lost %s on failed cleanup", name)
		}
	}
	if err := os.Remove(failure); err != nil {
		t.Fatal(err)
	}
	if err := run("down"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(state)
	if err != nil || len(entries) != 1 || entries[0].Name() != ".lifecycle" {
		t.Fatal("unexpected remaining state", entries, err)
	}
}

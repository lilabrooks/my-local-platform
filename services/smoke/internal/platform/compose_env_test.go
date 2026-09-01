package platform

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestComposeEnvRunnerPreservesDotenvValuesAndFiltersKeys(t *testing.T) {
	t.Parallel()

	binDir := t.TempDir()
	docker := filepath.Join(binDir, "docker")
	fakeOutput := `#!/bin/sh
printf '%s\n' \
  'HASHED=left#right' \
  'DOLLAR=$five' \
  'SPACED=two words' \
  'EMPTY=' \
  'AFTER=after value' \
  'UNREQUESTED_COMPOSE_VALUE=do-not-pass'
`
	if err := os.WriteFile(docker, []byte(fakeOutput), 0o755); err != nil {
		t.Fatal(err)
	}

	script := filepath.Join("..", "..", "..", "..", "scripts", "with-compose-env.py")
	command := exec.Command(
		"python3", script,
		"HASHED", "DOLLAR", "SPACED", "EMPTY", "AFTER", "MISSING",
		"--", "sh", "-c",
		`printf '%s\n' "$HASHED" "$DOLLAR" "$SPACED" "${EMPTY+set}:${EMPTY}" "$AFTER" "${MISSING-unset}" "${UNREQUESTED_COMPOSE_VALUE-unset}"`,
	)

	filtered := make([]string, 0, len(os.Environ())+2)
	for _, item := range os.Environ() {
		if strings.HasPrefix(item, "PATH=") ||
			strings.HasPrefix(item, "MISSING=") ||
			strings.HasPrefix(item, "UNREQUESTED_COMPOSE_VALUE=") {
			continue
		}
		filtered = append(filtered, item)
	}
	command.Env = append(filtered,
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"MLP_ENV_FILE=.env.example",
		"MISSING=inherited",
	)

	got, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("with-compose-env.py failed: %v\n%s", err, got)
	}
	want := "left#right\n$five\ntwo words\nset:\nafter value\nunset\nunset\n"
	if string(got) != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

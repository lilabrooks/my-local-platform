from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).parents[2]
LINT = ROOT / "scripts" / "lint.sh"
CHECKS_DIGEST = "sha256:1583562f8b90ed2a071b99f0e5ffff6b57e4ceb6ca3e4796577b4e6a339eb74c"
CHECKS_REPOSITORY = f"mirror.gcr.io/aquasec/trivy-checks@{CHECKS_DIGEST}"
MASKED_TOOLS = (
    "actionlint",
    "gitleaks",
    "go",
    "gofmt",
    "golangci-lint",
    "hadolint",
    "markdownlint-cli2",
    "ruff",
    "shellcheck",
    "terraform",
    "tflint",
    "yamllint",
)
TRIVY_TEST_VARIABLES = (
    "TRIVY_TEST_CONFIG_NONZERO",
    "TRIVY_TEST_EMBEDDED_FALLBACK",
    "TRIVY_TEST_EMPTY_CONTENT",
    "TRIVY_TEST_WRONG_DIGEST",
)

NATIVE_TRIVY_STUB = r"""
    #!/bin/sh
    if [ "${1:-}" = "--version" ]; then
      echo "Version: 0.74.0"
      exit 0
    fi

    command=$1
    shift
    printf '%s' "$command" >> "$TRIVY_TEST_LOG"
    for arg in "$@"; do printf ' %s' "$arg" >> "$TRIVY_TEST_LOG"; done
    printf '\n' >> "$TRIVY_TEST_LOG"

    cache=
    digest=
    set -- "$@"
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --cache-dir) cache=$2; shift 2 ;;
        --checks-bundle-repository) digest=${2##*@}; shift 2 ;;
        *) shift ;;
      esac
    done

    case "$command" in
      image) exit 0 ;;
      config)
        [ -z "${TRIVY_TEST_CONFIG_NONZERO:-}" ] || {
          echo "registry unavailable" >&2
          exit 1
        }
        # Trivy 0.74.0 returns zero after this fallback and leaves no bundle.
        [ -z "${TRIVY_TEST_EMBEDDED_FALLBACK:-}" ] || exit 0
        # Nor does it download again while the metadata names the requested
        # digest, whatever policy/content still holds.
        if grep -Fq "\"Digest\":\"$digest\"" "$cache/policy/metadata.json" 2>/dev/null; then
          exit 0
        fi
        [ -z "${TRIVY_TEST_WRONG_DIGEST:-}" ] || \
          digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
        # A download replaces policy/content instead of extracting into it.
        rm -rf "$cache/policy/content"
        mkdir -p "$cache/policy/content"
        [ -n "${TRIVY_TEST_EMPTY_CONTENT:-}" ] || \
          printf 'package checks\n' > "$cache/policy/content/check.rego"
        printf '{"Digest":"%s"}\n' "$digest" > "$cache/policy/metadata.json"
        ;;
      fs) test -d "$cache/policy/content" ;;
      *) exit 2 ;;
    esac
"""

DOCKER_STUB = r"""
    #!/bin/sh
    [ "${1:-}" = "info" ] && exit 0

    is_trivy=
    cache=
    command=
    digest=
    previous=
    for arg in "$@"; do
      [ "$arg" = "aquasec/trivy:0.74.0" ] && is_trivy=1
      case "$arg" in
        *:/trivy-cache) cache=${arg%:/trivy-cache} ;;
        image|config|fs) command=$arg ;;
      esac
      [ "$previous" = "--checks-bundle-repository" ] && digest=${arg##*@}
      previous=$arg
    done
    [ -n "$is_trivy" ] || exit 0

    printf '%s' "$command" >> "$TRIVY_TEST_LOG"
    for arg in "$@"; do printf ' %s' "$arg" >> "$TRIVY_TEST_LOG"; done
    printf '\n' >> "$TRIVY_TEST_LOG"
    case "$command" in
      image) exit 0 ;;
      config)
        [ -z "${TRIVY_TEST_CONFIG_NONZERO:-}" ] || {
          echo "registry unavailable" >&2
          exit 1
        }
        [ -z "${TRIVY_TEST_EMBEDDED_FALLBACK:-}" ] || exit 0
        if grep -Fq "\"Digest\":\"$digest\"" "$cache/policy/metadata.json" 2>/dev/null; then
          exit 0
        fi
        [ -z "${TRIVY_TEST_WRONG_DIGEST:-}" ] || \
          digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
        rm -rf "$cache/policy/content"
        mkdir -p "$cache/policy/content"
        [ -n "${TRIVY_TEST_EMPTY_CONTENT:-}" ] || \
          printf 'package checks\n' > "$cache/policy/content/check.rego"
        printf '{"Digest":"%s"}\n' "$digest" > "$cache/policy/metadata.json"
        ;;
      fs) test -d "$cache/policy/content" ;;
      *) exit 2 ;;
    esac
"""

NOTE = r"NOTE\x1b\[0m  trivy -- cached checks bundle had pinned metadata but no policies"


class TrivyLintTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.temp_path = Path(self.temp.name)
        self.bin_path = self.temp_path / "bin"
        self.bin_path.mkdir()
        self.log_path = self.temp_path / "trivy.log"
        self.cache_path = self.temp_path / f"mlp-trivy-cache-{CHECKS_DIGEST.removeprefix('sha256:')}"

        blocker = "#!/bin/sh\nexit 127\n"
        for name in MASKED_TOOLS:
            self.write_tool(name, blocker)
        # Keep unrelated branches deterministic and offline.
        self.write_tool("npx", "#!/bin/sh\necho 'markdownlint test stub'\n")
        self.write_tool("sleep", "#!/bin/sh\nexit 0\n")

        self.env = os.environ.copy()
        for name in ("LINT_STRICT", "LINT_SKIP_OK", *TRIVY_TEST_VARIABLES):
            self.env.pop(name, None)
        self.env.update(
            {
                "PATH": f"{self.bin_path}:/usr/bin:/bin",
                "TMPDIR": str(self.temp_path),
                "TRIVY_TEST_LOG": str(self.log_path),
            }
        )

    def write_tool(self, name: str, body: str) -> None:
        path = self.bin_path / name
        path.write_text(textwrap.dedent(body))
        path.chmod(0o755)

    def install_native_trivy(self) -> None:
        self.write_tool("docker", "#!/bin/sh\nexit 1\n")
        self.write_tool("trivy", NATIVE_TRIVY_STUB)

    def install_container_trivy(self) -> None:
        self.write_tool("docker", DOCKER_STUB)
        self.write_tool("trivy", "#!/bin/sh\nexit 127\n")

    def seed_database_cache(self) -> Path:
        db_marker = self.cache_path / "db" / "keep"
        db_marker.parent.mkdir(parents=True)
        db_marker.write_text("cached database\n")
        return db_marker

    def seed_checks_cache(self, cache: Path, digest: str, *, policies: bool) -> Path:
        # Without policies this is the state seen under the macOS $TMPDIR: the
        # files gone, their directories and Trivy's metadata left behind.
        content = cache / "policy" / "content" / "kubernetes"
        content.mkdir(parents=True)
        if policies:
            (content / "check.rego").write_text("package checks\n")
        metadata = cache / "policy" / "metadata.json"
        metadata.write_text(
            json.dumps(
                {"Digest": digest, "DownloadedAt": "2026-09-19T00:00:00Z", "MajorVersion": 2},
                separators=(",", ":"),
            )
            + "\n"
        )
        return metadata

    def run_lint(self, **extra_env: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["/bin/bash", str(LINT)],
            cwd=ROOT,
            env=self.env | extra_env,
            text=True,
            capture_output=True,
            check=False,
        )

    def log_lines(self) -> list[str]:
        return self.log_path.read_text().splitlines()

    def assert_refresh_then_scan(self, result: subprocess.CompletedProcess[str]) -> None:
        lines = self.log_lines()
        self.assertEqual(
            [line.split()[0] for line in lines],
            ["image", "config", "fs"],
        )
        for line in lines:
            self.assertIn(str(self.cache_path), line)
        self.assertIn(CHECKS_REPOSITORY, lines[1].split())
        self.assertIn(CHECKS_REPOSITORY, lines[2].split())
        self.assertIn("--ignorefile", lines[2].split())
        self.assertIn(".trivyignore.yaml", lines[2].split())
        self.assertRegex(result.stdout, r"PASS\x1b\[0m  trivy")

    def assert_refresh_rejected(
        self,
        result: subprocess.CompletedProcess[str],
        detail: str,
    ) -> None:
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(
            [line.split()[0] for line in self.log_lines()],
            ["image", "config", "config", "config"],
        )
        self.assertIn("checks bundle refresh failed after 3 attempts", result.stdout)
        self.assertIn(detail, result.stdout)
        self.assertRegex(result.stdout, r"FAIL\x1b\[0m  trivy")

    def test_native_trivy_uses_the_pinned_bundle_and_keeps_the_cache(self):
        db_marker = self.seed_database_cache()
        self.install_native_trivy()

        result = self.run_lint()

        self.assert_refresh_then_scan(result)
        self.assertTrue(db_marker.exists())

    def test_native_zero_exit_embedded_fallback_is_rejected(self):
        self.install_native_trivy()

        result = self.run_lint(TRIVY_TEST_EMBEDDED_FALLBACK="1")

        self.assert_refresh_rejected(result, "unverified checks rejected")

    def test_container_trivy_uses_the_pinned_bundle_and_keeps_the_cache(self):
        db_marker = self.seed_database_cache()
        self.install_container_trivy()

        result = self.run_lint()

        self.assert_refresh_then_scan(result)
        self.assertTrue(db_marker.exists())

    def test_container_zero_exit_embedded_fallback_is_rejected(self):
        self.install_container_trivy()

        result = self.run_lint(TRIVY_TEST_EMBEDDED_FALLBACK="1")

        self.assert_refresh_rejected(result, "unverified checks rejected")

    def test_mismatched_cached_digest_is_rejected(self):
        self.install_native_trivy()

        result = self.run_lint(TRIVY_TEST_WRONG_DIGEST="1")

        self.assert_refresh_rejected(result, "unverified checks rejected")

    def test_metadata_valid_empty_content_is_rejected(self):
        self.install_native_trivy()

        result = self.run_lint(TRIVY_TEST_EMPTY_CONTENT="1")

        self.assert_refresh_rejected(result, "unverified checks rejected")

    def test_nonzero_bundle_refresh_is_retried_and_rejected(self):
        self.install_native_trivy()

        result = self.run_lint(TRIVY_TEST_CONFIG_NONZERO="1")

        self.assert_refresh_rejected(result, "registry unavailable")

    def test_native_cached_metadata_without_checks_is_downloaded_again(self):
        db_marker = self.seed_database_cache()
        self.seed_checks_cache(self.cache_path, CHECKS_DIGEST, policies=False)
        other = self.seed_checks_cache(
            self.temp_path / f"mlp-trivy-cache-{'b' * 64}",
            f"sha256:{'b' * 64}",
            policies=False,
        )
        other_metadata = other.read_text()
        self.install_native_trivy()

        result = self.run_lint()

        self.assert_refresh_then_scan(result)
        self.assertEqual(len(re.findall(NOTE, result.stdout)), 1)
        self.assertTrue(db_marker.exists())
        # Only the cache named for this pin is repaired.
        self.assertEqual(other.read_text(), other_metadata)

    def test_container_cached_metadata_without_checks_is_downloaded_again(self):
        db_marker = self.seed_database_cache()
        self.seed_checks_cache(self.cache_path, CHECKS_DIGEST, policies=False)
        self.install_container_trivy()

        result = self.run_lint()

        self.assert_refresh_then_scan(result)
        self.assertEqual(len(re.findall(NOTE, result.stdout)), 1)
        self.assertTrue(db_marker.exists())

    def test_download_after_the_repair_is_still_verified(self):
        self.seed_checks_cache(self.cache_path, CHECKS_DIGEST, policies=False)
        self.install_native_trivy()

        result = self.run_lint(TRIVY_TEST_EMBEDDED_FALLBACK="1")

        self.assert_refresh_rejected(result, "unverified checks rejected")
        self.assertEqual(len(re.findall(NOTE, result.stdout)), 1)
        self.assertFalse((self.cache_path / "policy" / "metadata.json").exists())

    def test_complete_cached_bundle_is_left_alone(self):
        metadata = self.seed_checks_cache(self.cache_path, CHECKS_DIGEST, policies=True)
        cached = metadata.read_text()
        self.install_native_trivy()

        result = self.run_lint()

        self.assert_refresh_then_scan(result)
        self.assertNotRegex(result.stdout, NOTE)
        self.assertEqual(metadata.read_text(), cached)


if __name__ == "__main__":
    unittest.main()

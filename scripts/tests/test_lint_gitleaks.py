from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).parents[2]
LINT = ROOT / "scripts" / "lint.sh"
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
    "trivy",
    "yamllint",
)
GITLEAKS_TEST_VARIABLES = (
    "GITLEAKS_TEST_EXIT",
    "GITLEAKS_TEST_LOG",
    "GITLEAKS_TEST_OUTPUT",
)

# Gitleaks 8.30.1 colors its log lines even when they are captured. The first
# failure below is what it printed in a linked worktree before lint.sh mounted
# the common git directory: git failed, and Gitleaks still exited 0.
SCANNED = (
    "\x1b[32mINF\x1b[0m \x1b[1m150 commits scanned.\x1b[0m\n"
    "\x1b[32mINF\x1b[0m \x1b[1mno leaks found\x1b[0m\n"
)
ZERO_SCANNED = (
    "\x1b[31mERR\x1b[0m \x1b[1m[git] fatal: not a git repository: "
    "/outside/the/mount/.git/worktrees/example\x1b[0m\n"
    "\x1b[32mINF\x1b[0m \x1b[1m0 commits scanned.\x1b[0m\n"
    "\x1b[32mINF\x1b[0m \x1b[1mno leaks found\x1b[0m\n"
)
NO_COUNT = "\x1b[32mINF\x1b[0m \x1b[1mno leaks found\x1b[0m\n"
LEAK_FOUND = (
    "\x1b[32mINF\x1b[0m \x1b[1m150 commits scanned.\x1b[0m\n"
    "\x1b[33mWRN\x1b[0m \x1b[1mleaks found: 1\x1b[0m\n"
)

# Logs each argument of a Gitleaks container run on its own line and prints the
# output the test chose. Every other image succeeds silently.
DOCKER_STUB = r"""
    #!/bin/sh
    [ "${1:-}" = "info" ] && exit 0
    for arg in "$@"; do
      if [ "$arg" = "zricethezav/gitleaks:v8.30.1" ]; then
        for logged in "$@"; do printf '%s\n' "$logged"; done >> "$GITLEAKS_TEST_LOG"
        printf '%s' "$GITLEAKS_TEST_OUTPUT"
        exit "${GITLEAKS_TEST_EXIT:-0}"
      fi
    done
    exit 0
"""

NATIVE_GITLEAKS_STUB = r"""
    #!/bin/sh
    if [ "${1:-}" = "version" ]; then
      echo "8.30.1"
      exit 0
    fi
    for arg in "$@"; do printf '%s\n' "$arg"; done >> "$GITLEAKS_TEST_LOG"
    printf '%s' "$GITLEAKS_TEST_OUTPUT"
    exit "${GITLEAKS_TEST_EXIT:-0}"
"""


def git_common_dir() -> str:
    return subprocess.run(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()


class GitleaksLintTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.temp_path = Path(self.temp.name)
        self.bin_path = self.temp_path / "bin"
        self.bin_path.mkdir()
        self.log_path = self.temp_path / "gitleaks.log"

        blocker = "#!/bin/sh\nexit 127\n"
        for name in MASKED_TOOLS:
            self.write_tool(name, blocker)
        # Keep unrelated branches deterministic and offline.
        self.write_tool("npx", "#!/bin/sh\necho 'markdownlint test stub'\n")
        self.write_tool("sleep", "#!/bin/sh\nexit 0\n")

        self.env = os.environ.copy()
        for name in ("LINT_STRICT", "LINT_SKIP_OK", *GITLEAKS_TEST_VARIABLES):
            self.env.pop(name, None)
        self.env.update(
            {
                "PATH": f"{self.bin_path}:/usr/bin:/bin",
                "TMPDIR": str(self.temp_path),
                "GITLEAKS_TEST_LOG": str(self.log_path),
            }
        )

    def write_tool(self, name: str, body: str) -> None:
        path = self.bin_path / name
        path.write_text(textwrap.dedent(body).lstrip("\n"))
        path.chmod(0o755)

    def install_container_gitleaks(self) -> None:
        self.write_tool("docker", DOCKER_STUB)

    def install_native_gitleaks(self) -> None:
        # No Docker, so every other check skips and the exit code is gitleaks'.
        self.write_tool("docker", "#!/bin/sh\nexit 1\n")
        self.write_tool("gitleaks", NATIVE_GITLEAKS_STUB)

    def run_lint(
        self,
        output: str,
        exit_code: int = 0,
        lint: Path = LINT,
        **extra_env: str,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["/bin/bash", str(lint)],
            cwd=lint.parents[1],
            env=self.env
            | {"GITLEAKS_TEST_OUTPUT": output, "GITLEAKS_TEST_EXIT": str(exit_code)}
            | extra_env,
            text=True,
            capture_output=True,
            check=False,
        )

    def logged_args(self) -> list[str]:
        return self.log_path.read_text().splitlines()

    def assert_rejected(self, result: subprocess.CompletedProcess[str], scanned: str) -> None:
        self.assertRegex(result.stdout, r"FAIL\x1b\[0m  gitleaks")
        self.assertIn(
            f"Gitleaks exited 0 after scanning {scanned} commits, "
            "but `git rev-list --count HEAD` gives",
            result.stdout,
        )

    def test_container_mounts_the_common_git_directory_read_only(self):
        self.install_container_gitleaks()

        result = self.run_lint(SCANNED)

        args = self.logged_args()
        common = git_common_dir()
        mount = f"{common}:{common}:ro"
        self.assertIn(mount, args)
        self.assertEqual(args[args.index(mount) - 1], "-v")
        self.assertIn("--redact", args)
        self.assertRegex(result.stdout, r"PASS\x1b\[0m  gitleaks")

    def test_container_zero_commit_pass_is_rejected(self):
        self.install_container_gitleaks()

        result = self.run_lint(ZERO_SCANNED)

        self.assert_rejected(result, "0")
        self.assertIn("[git] fatal: not a git repository", result.stdout)

    def test_container_findings_still_fail(self):
        self.install_container_gitleaks()

        result = self.run_lint(LEAK_FOUND, exit_code=1)

        self.assertRegex(result.stdout, r"FAIL\x1b\[0m  gitleaks")
        self.assertIn("leaks found: 1", result.stdout)
        self.assertNotIn("Gitleaks exited 0", result.stdout)

    def test_native_scan_with_commits_passes(self):
        self.install_native_gitleaks()

        result = self.run_lint(SCANNED)

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("--redact", self.logged_args())
        self.assertRegex(result.stdout, r"PASS\x1b\[0m  gitleaks")

    def test_native_zero_commit_pass_is_rejected(self):
        self.install_native_gitleaks()

        result = self.run_lint(ZERO_SCANNED)

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assert_rejected(result, "0")

    def test_missing_commit_count_is_rejected(self):
        self.install_native_gitleaks()

        result = self.run_lint(NO_COUNT)

        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assert_rejected(result, "no")

    def test_checkout_without_git_history_is_rejected(self):
        checkout = self.temp_path / "export"
        (checkout / "scripts").mkdir(parents=True)
        lint = checkout / "scripts" / "lint.sh"
        shutil.copy2(LINT, lint)
        # lint.sh expands its Dockerfile list, and macOS bash 3.2 aborts on an
        # empty array under `set -u`.
        (checkout / "Dockerfile").write_text("FROM scratch\n")
        self.install_container_gitleaks()

        result = self.run_lint(
            SCANNED,
            lint=lint,
            GIT_CEILING_DIRECTORIES=str(self.temp_path),
        )

        self.assertRegex(result.stdout, r"FAIL\x1b\[0m  gitleaks")
        self.assertIn("cannot find the git directory to scan", result.stdout)
        self.assertFalse(self.log_path.exists())


if __name__ == "__main__":
    unittest.main()

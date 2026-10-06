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

# Each native stub reports the version lint.sh pins, so lint.sh takes the native
# path, and otherwise logs its working directory and arguments.
NATIVE_VERSIONS = {
    "yamllint": "yamllint 1.37.1",
    "ruff": "ruff 0.16.6",
    "shellcheck": "version: 0.11.0",
    "markdownlint-cli2": "markdownlint-cli2 v0.23.2",
    "actionlint": "1.7.12",
    "hadolint": "Haskell Dockerfile Linter 2.15.1",
    "terraform": "Terraform v1.16.3",
    "golangci-lint": "golangci-lint has version 2.13.1",
}
# Not under test here; masked so they skip rather than reach the network.
OTHER_TOOLS = ("gitleaks", "go", "gofmt", "tflint", "trivy", "npx")

NATIVE_STUB = r"""
    #!/bin/sh
    name=$(basename "$0")
    case "${1:-}" in
      --version|-version|version)
        printf '%s\n' VERSION
        exit 0
        ;;
    esac
    {
      printf '== %s\n' "$PWD"
      for arg in "$@"; do printf '%s\n' "$arg"; done
    } >> "$LINT_TEST_LOG/$name"
"""

# Logs the run of each pinned linter image under the tool's name. Hadolint's
# container reads the Dockerfile on stdin, so its log gets the contents, which
# the checkout makes each file's own path, marked "stdin: ". Everything else, Trivy's and
# TFLint's images included, succeeds silently.
DOCKER_STUB = r"""
    #!/bin/sh
    [ "${1:-}" = "info" ] && exit 0
    name=
    for arg in "$@"; do
      case "$arg" in
        pipelinecomponents/yamllint:0.35.10) name=yamllint ;;
        ghcr.io/astral-sh/ruff:0.16.6) name=ruff ;;
        koalaman/shellcheck:v0.11.0) name=shellcheck ;;
        davidanson/markdownlint-cli2:v0.23.2) name=markdownlint-cli2 ;;
        rhysd/actionlint:1.7.12) name=actionlint ;;
        hadolint/hadolint:v2.15.1-alpine) name=hadolint ;;
        hashicorp/terraform:1.16.3) name=terraform ;;
        golangci/golangci-lint:v2.13.1) name=golangci-lint ;;
      esac
    done
    [ -n "$name" ] || exit 0
    {
      printf '== container\n'
      for arg in "$@"; do printf '%s\n' "$arg"; done
      [ "$name" != hadolint ] || sed 's/^/stdin: /'
    } >> "$LINT_TEST_LOG/$name"
    exit 0
"""

# Every kind of file a linter reads, committed, untracked and ignored.
COMMITTED = (
    ".yamllint.yml",
    "a.yaml",
    "k8s/b.yml",
    "deleted.yaml",
    "tool.py",
    "ruff.toml",
    "scripts/run.sh",
    "README.md",
    ".github/workflows/ci.yml",
    "Dockerfile",
    "services/x/Dockerfile",
    "services/x/go.mod",
    "infra/terraform/main.tf",
)
# Untracked and covered by no ignore rule, so a commit would take them. Two
# names test quoting: one reads as an option, one as a glob.
UNTRACKED = (
    "new.yaml",
    "new.sh",
    "docs/new.md",
    "-dash.md",
    "we[ir]d.md",
    ".github/workflows/new.yml",
    ".github/workflows/sub/nested.yml",
)
IGNORED_BY_GITIGNORE = (
    ".evidence/probe/bad.yaml",
    ".evidence/probe/bad.md",
    ".evidence/probe/bad.sh",
    ".evidence/probe/bad.py",
    ".evidence/probe/Dockerfile",
    ".evidence/probe/go.mod",
    ".github/workflows/local-scratch.yml",
    "infra/terraform/terraform.tfvars",
)
# The agent checkouts on the host this was found on are excluded through
# .git/info/exclude, not .gitignore.
IGNORED_BY_EXCLUDE = (
    ".claude/worktrees/other/k8s/c.yaml",
    ".claude/worktrees/other/README.md",
    ".claude/worktrees/other/run.sh",
    ".claude/worktrees/other/tool.py",
    ".claude/worktrees/other/Dockerfile",
    ".claude/worktrees/other/k8s/validate/go.mod",
)

# What each linter must be handed: the committed and untracked files of its
# kind, with "./" in front, and nothing ignored or deleted.
EXPECTED = {
    "yamllint": [
        "./.github/workflows/ci.yml",
        "./.github/workflows/new.yml",
        "./.github/workflows/sub/nested.yml",
        "./.yamllint.yml",
        "./a.yaml",
        "./k8s/b.yml",
        "./new.yaml",
    ],
    "ruff": ["./ruff.toml", "./tool.py"],
    "shellcheck": ["./new.sh", "./scripts/lint.sh", "./scripts/run.sh"],
    "markdownlint-cli2": ["./-dash.md", "./README.md", "./docs/new.md", "./we[ir]d.md"],
    # Not the nested file: actionlint reads only the directory's own files.
    "actionlint": ["./.github/workflows/ci.yml", "./.github/workflows/new.yml"],
    "hadolint": ["./Dockerfile", "./services/x/Dockerfile"],
    "terraform": ["./infra/terraform/main.tf"],
    "golangci-lint": ["services/x"],
}


class LintFileListTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.temp_path = Path(self.temp.name)
        self.bin_path = self.temp_path / "bin"
        self.bin_path.mkdir()
        self.log_path = self.temp_path / "log"
        self.log_path.mkdir()

        blocker = "#!/bin/sh\nexit 127\n"
        for name in OTHER_TOOLS:
            self.write_tool(name, blocker)
        self.write_tool("sleep", "#!/bin/sh\nexit 0\n")

        self.env = os.environ.copy()
        for name in ("LINT_STRICT", "LINT_SKIP_OK"):
            self.env.pop(name, None)
        self.env.update(
            {
                "PATH": f"{self.bin_path}:/usr/bin:/bin",
                "TMPDIR": str(self.temp_path),
                "LINT_TEST_LOG": str(self.log_path),
                # Keep the host's excludes and signing out of the scratch checkout.
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_CONFIG_NOSYSTEM": "1",
            }
        )

    def write_tool(self, name: str, body: str) -> None:
        path = self.bin_path / name
        path.write_text(textwrap.dedent(body).lstrip("\n"))
        path.chmod(0o755)

    def install_native_linters(self) -> None:
        self.write_tool("docker", "#!/bin/sh\nexit 1\n")
        for name, version in NATIVE_VERSIONS.items():
            self.write_tool(name, NATIVE_STUB.replace("VERSION", f"'{version}'"))

    def install_container_linters(self) -> None:
        self.write_tool("docker", DOCKER_STUB)
        for name in NATIVE_VERSIONS:
            self.write_tool(name, "#!/bin/sh\nexit 127\n")

    def write_files(self, repo: Path, names: tuple[str, ...]) -> None:
        for name in names:
            (repo / name).parent.mkdir(parents=True, exist_ok=True)
            # Its own path, so a linter that reads stdin still says which file.
            (repo / name).write_text(f"{name}\n")

    def git(self, repo: Path, *args: str) -> None:
        subprocess.run(
            ["git", "-C", str(repo), "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
            env=self.env,
            check=True,
        )

    def make_checkout(self, committed: tuple[str, ...] = COMMITTED) -> Path:
        repo = self.temp_path / "checkout"
        (repo / "scripts").mkdir(parents=True)
        shutil.copy2(LINT, repo / "scripts" / "lint.sh")
        (repo / ".gitignore").write_text(".evidence/\n*.tfvars\n/.github/workflows/local-*\n")
        self.write_files(repo, committed)
        for args in (["init", "-q"], ["add", "-A"], ["commit", "-q", "--no-gpg-sign", "-m", "init"]):
            self.git(repo, *args)
        return repo

    def add_working_tree_changes(self, repo: Path, untracked: tuple[str, ...] = UNTRACKED) -> None:
        (repo / "deleted.yaml").unlink(missing_ok=True)
        self.write_files(repo, untracked + IGNORED_BY_GITIGNORE + IGNORED_BY_EXCLUDE)
        with (repo / ".git" / "info" / "exclude").open("a") as exclude:
            exclude.write(".claude/worktrees/\n")

    def run_lint(self, repo: Path, **extra_env: str) -> subprocess.CompletedProcess[str]:
        # /bin/bash: macOS's is 3.2, which the script must keep working with.
        return subprocess.run(
            ["/bin/bash", str(repo / "scripts" / "lint.sh")],
            cwd=repo,
            env=self.env | extra_env,
            text=True,
            capture_output=True,
            check=False,
        )

    def handed(self, name: str, repo: Path) -> list[str]:
        """The files lint.sh handed one linter, across all its runs."""
        log = self.log_path / name
        self.assertTrue(log.exists(), f"{name} did not run")
        lines = log.read_text().splitlines()
        if name == "golangci-lint":
            # A module is the directory the linter runs in.
            dirs = []
            for i, line in enumerate(lines):
                if line.startswith("== /") and line != "== container":
                    dirs.append(str(Path(line[3:]).resolve().relative_to(repo.resolve())))
                elif line == "-w":
                    dirs.append(lines[i + 1].removeprefix("/repo/"))
            return sorted(dirs)
        if name == "hadolint" and "== container" in lines:
            # The container reads each Dockerfile on stdin.
            return sorted(f"./{line[7:]}" for line in lines if line.startswith("stdin: "))
        files = []
        for line in lines:
            if line.startswith("./"):
                files.append(line)
            elif line.startswith(":./"):
                # markdownlint-cli2: ":" makes the argument a literal path.
                files.append(line[1:])
        return sorted(files)

    def assert_handed_commit_view(self, repo: Path) -> None:
        for name, expected in EXPECTED.items():
            with self.subTest(linter=name):
                self.assertEqual(self.handed(name, repo), expected)

    def test_native_linters_read_what_a_commit_would_take(self):
        repo = self.make_checkout()
        self.add_working_tree_changes(repo)
        self.install_native_linters()

        result = self.run_lint(repo)

        self.assert_handed_commit_view(repo)
        self.assertIn("--no-globs", (self.log_path / "markdownlint-cli2").read_text().splitlines())
        self.assertIn("--force-exclude", (self.log_path / "ruff").read_text().splitlines())
        for name in ("yamllint", "ruff", "shellcheck", "markdownlint", "actionlint", "hadolint", "terraform fmt", "golangci-lint"):
            self.assertRegex(result.stdout, rf"PASS\x1b\[0m  {name}\n")

    def test_container_linters_read_what_a_commit_would_take(self):
        repo = self.make_checkout()
        self.add_working_tree_changes(repo)
        self.install_container_linters()

        result = self.run_lint(repo)

        self.assert_handed_commit_view(repo)
        self.assertIn("--no-globs", (self.log_path / "markdownlint-cli2").read_text().splitlines())
        self.assertIn("--force-exclude", (self.log_path / "ruff").read_text().splitlines())
        for name in ("yamllint", "ruff", "shellcheck", "markdownlint", "actionlint", "hadolint", "terraform fmt", "golangci-lint"):
            self.assertRegex(result.stdout, rf"PASS\x1b\[0m  {name}\n")

    def test_a_linter_with_no_files_skips_instead_of_aborting(self):
        # Only lint.sh itself: every list but shellcheck's is empty. bash 3.2
        # aborts on an empty "${list[@]}" under `set -u`, and a linter handed
        # no arguments would walk the whole tree.
        repo = self.make_checkout(committed=())
        self.add_working_tree_changes(repo, untracked=())
        self.install_native_linters()

        result = self.run_lint(repo)

        # It ran to the end. The exit status is not the point: the checkout has
        # no ADR index for that check to find.
        self.assertNotIn("unbound variable", result.stderr)
        self.assertRegex(result.stdout, r"passed \d+, failed \d+, skipped \d+")
        self.assertEqual(self.handed("shellcheck", repo), ["./scripts/lint.sh"])
        for name, what in (
            ("yamllint", "YAML files"),
            ("ruff", "Python files"),
            ("markdownlint", "Markdown files"),
            ("actionlint", "workflow files"),
            ("hadolint", "Dockerfiles"),
            ("terraform fmt", "Terraform files under infra/terraform"),
            ("golangci-lint", "Go modules"),
        ):
            self.assertIn(f"SKIP\x1b[0m  {name} -- no {what} in the files a commit would take\n", result.stdout)
        for name in ("yamllint", "ruff", "markdownlint-cli2", "actionlint", "hadolint", "terraform", "golangci-lint"):
            self.assertFalse((self.log_path / name).exists(), f"{name} ran with no files")

        strict = self.run_lint(repo, LINT_STRICT="1")

        self.assertRegex(strict.stdout, r"FAIL\x1b\[0m  yamllint -- no YAML files")

    def test_outside_git_it_stops_before_linting(self):
        tree = self.temp_path / "not-a-checkout"
        (tree / "scripts").mkdir(parents=True)
        shutil.copy2(LINT, tree / "scripts" / "lint.sh")
        self.write_files(tree, ("a.yaml",))
        self.install_native_linters()
        env = self.env | {"GIT_CEILING_DIRECTORIES": str(self.temp_path)}

        result = subprocess.run(
            ["/bin/bash", str(tree / "scripts" / "lint.sh")],
            cwd=tree,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 1)
        self.assertIn("reads the files to lint from git, which failed", result.stderr)
        self.assertEqual(list(self.log_path.iterdir()), [])


if __name__ == "__main__":
    unittest.main()

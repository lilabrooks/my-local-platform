#!/usr/bin/env bash
# Run every linter in the repository.
#
# Each linter runs from a local binary when one is installed, and falls back to
# a pinned container otherwise, so this works on a clean machine with only
# Docker. Anything that can run neither way is reported as SKIP rather than
# silently passing -- a linter that did not run is not a linter that passed.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

PASS=0 FAIL=0 SKIP=0
FAILED=()

green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }
amber() { printf '\033[33m%s\033[0m' "$1"; }

# report <name> <exit-code> <output>
report() {
  local name="$1" code="$2" out="$3"
  if [ "$code" -eq 0 ]; then
    printf '  %s  %s\n' "$(green PASS)" "$name"; PASS=$((PASS + 1))
  else
    printf '  %s  %s\n' "$(red FAIL)" "$name"; FAIL=$((FAIL + 1)); FAILED+=("$name")
    [ -n "$out" ] && printf '%s\n' "$out" | sed 's/^/        /' | head -25
  fi
}

skip() {
  printf '  %s  %s -- %s\n' "$(amber SKIP)" "$1" "$2"; SKIP=$((SKIP + 1))
}

has()        { command -v "$1" >/dev/null 2>&1; }
has_docker() { docker info >/dev/null 2>&1; }

echo "linting $(pwd)"
echo

# --- YAML -------------------------------------------------------------------
if has yamllint; then
  out=$(yamllint -f parsable . 2>&1); report "yamllint" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/data -w /data pipelinecomponents/yamllint:latest \
        yamllint -f parsable . 2>&1); report "yamllint" $? "$out"
else
  skip "yamllint" "install with: brew install yamllint"
fi

# --- Shell ------------------------------------------------------------------
# Not `mapfile`: macOS ships bash 3.2, which does not have it.
SCRIPTS=()
while IFS= read -r f; do SCRIPTS+=("$f"); done < <(
  find . -name '*.sh' -not -path '*/.terraform/*' | sort
)
if has shellcheck; then
  out=$(shellcheck "${SCRIPTS[@]}" 2>&1); report "shellcheck" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/mnt -w /mnt koalaman/shellcheck:stable \
        "${SCRIPTS[@]}" 2>&1); report "shellcheck" $? "$out"
else
  skip "shellcheck" "install with: brew install shellcheck"
fi

# --- Markdown ---------------------------------------------------------------
if has markdownlint-cli2; then
  out=$(markdownlint-cli2 2>&1); report "markdownlint" $? "$out"
elif has npx; then
  out=$(npx --yes markdownlint-cli2 2>&1 | grep -vE '^npm notice'); code=$?
  report "markdownlint" "$code" "$out"
else
  skip "markdownlint" "needs npx or markdownlint-cli2"
fi

# --- GitHub Actions ---------------------------------------------------------
if has actionlint; then
  out=$(actionlint 2>&1); report "actionlint" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:latest 2>&1)
  report "actionlint" $? "$out"
else
  skip "actionlint" "needs docker or: brew install actionlint"
fi

# --- Dockerfiles ------------------------------------------------------------
if has hadolint; then
  out=$(hadolint services/echo/Dockerfile 2>&1); report "hadolint" $? "$out"
elif has_docker; then
  out=$(docker run --rm -i hadolint/hadolint:latest-alpine hadolint - \
        < services/echo/Dockerfile 2>&1); report "hadolint" $? "$out"
else
  skip "hadolint" "needs docker or: brew install hadolint"
fi

# --- Terraform --------------------------------------------------------------
tf_fail=0 tf_out=""
if has terraform; then
  out=$(terraform fmt -check -recursive infra/terraform 2>&1) || {
    tf_fail=1; tf_out="not formatted:\n$out"
  }
  report "terraform fmt" "$tf_fail" "$tf_out"
else
  skip "terraform fmt" "terraform not installed"
fi

if has_docker; then
  lint_fail=0 lint_out=""
  for stack in infra/terraform/bootstrap infra/terraform/envs/dev; do
    out=$(docker run --rm -v "$PWD":/data -w "/data/$stack" \
      -e TFLINT_CONFIG_FILE=/data/.tflint.hcl \
      --entrypoint sh ghcr.io/terraform-linters/tflint:latest \
      -c 'tflint --init >/dev/null 2>&1 && tflint --format compact' 2>&1) || {
        lint_fail=1; lint_out="${lint_out}\n${stack}:\n${out}"
      }
  done
  report "tflint" "$lint_fail" "$lint_out"
else
  skip "tflint" "needs docker"
fi

# --- Secrets ----------------------------------------------------------------
# Runs last: it is the one whose failure should be impossible to miss.
if has gitleaks; then
  out=$(gitleaks detect --source=. --no-banner --redact 2>&1); report "gitleaks" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/repo -w /repo zricethezav/gitleaks:latest \
        detect --source=. --no-banner --redact 2>&1); report "gitleaks" $? "$out"
else
  skip "gitleaks" "needs docker or: brew install gitleaks"
fi

echo
printf 'passed %d, failed %d, skipped %d\n' "$PASS" "$FAIL" "$SKIP"
if [ "$FAIL" -gt 0 ]; then
  printf 'failing: %s\n' "${FAILED[*]}"
  exit 1
fi

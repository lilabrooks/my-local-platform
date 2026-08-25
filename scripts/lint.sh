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
  out=$(docker run --rm -v "$PWD":/data -w /data pipelinecomponents/yamllint:0.35.10 \
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
  out=$(docker run --rm -v "$PWD":/mnt -w /mnt koalaman/shellcheck:v0.11.0 \
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
  out=$(docker run --rm -v "$PWD":/repo -w /repo rhysd/actionlint:1.7.12 2>&1)
  report "actionlint" $? "$out"
else
  skip "actionlint" "needs docker or: brew install actionlint"
fi

# --- Dockerfiles ------------------------------------------------------------
if has hadolint; then
  out=$(hadolint services/echo/Dockerfile 2>&1); report "hadolint" $? "$out"
elif has_docker; then
  out=$(docker run --rm -i hadolint/hadolint:v2.15.1-alpine hadolint - \
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
  # Cache the downloaded ruleset between runs. Without this every run refetches
  # the plugin from the GitHub API, which is slow and rate-limited.
  TFLINT_CACHE="${TMPDIR:-/tmp}/mlp-tflint-plugins"
  mkdir -p "$TFLINT_CACHE"

  lint_fail=0 lint_out=""
  for stack in infra/terraform/bootstrap infra/terraform/envs/dev; do
    # --init is NOT silenced. Hiding it once turned a GitHub 504 into an empty
    # failure with no cause, which cost more time than the flake itself.
    out=$(docker run --rm \
      -v "$PWD":/data -v "$TFLINT_CACHE":/root/.tflint.d \
      -w "/data/$stack" \
      -e TFLINT_CONFIG_FILE=/data/.tflint.hcl \
      --entrypoint sh ghcr.io/terraform-linters/tflint:v0.64.0 \
      -c 'tflint --init && tflint --format compact' 2>&1) || {
        lint_fail=1
        lint_out="${lint_out}${stack}:
${out}
"
      }
  done

  # A plugin download failure is the network, not the code. Say so, because the
  # fix is "retry", not "edit terraform".
  if [ "$lint_fail" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'Failed to fetch GitHub releases'; then
    skip "tflint" "plugin download failed (GitHub API); retry when it recovers"
  else
    report "tflint" "$lint_fail" "$lint_out"
  fi
else
  skip "tflint" "needs docker"
fi

# --- Go ---------------------------------------------------------------------
# go vet is a thin net. golangci-lint bundles errcheck, staticcheck, unused and
# ineffassign, which is what actually catches unchecked errors and dead code.
#
# Modules are discovered rather than listed. A hardcoded list silently skipped
# services/relay when it was added, and reported PASS -- a lint run that does
# not lint the new code is worse than one that fails.
if has golangci-lint; then
  go_fail=0 go_out=""
  go_mods=$(find . -name go.mod -not -path './*/.terraform/*' -not -path './*/node_modules/*' \
            -exec dirname {} \; | sed 's|^\./||' | sort)
  for mod in $go_mods; do
    out=$(cd "$mod" && golangci-lint run --config ../../.golangci.yml --timeout 5m 2>&1) || {
      go_fail=1
      go_out="${go_out}${mod}:
${out}
"
    }
  done
  report "golangci-lint" "$go_fail" "$go_out"
else
  skip "golangci-lint" "install with: brew install golangci-lint"
fi

# --- Infrastructure security ------------------------------------------------
# tflint checks that Terraform is valid; trivy checks whether it is safe. They
# overlap not at all -- trivy found six issues tflint passed clean.
# --skip-dirs matters: .terraform/ holds vendored upstream modules whose
# example manifests are not ours to fix.
if has trivy; then
  out=$(trivy fs --scanners misconfig,secret \
        --severity MEDIUM,HIGH,CRITICAL \
        --skip-dirs '**/.terraform' \
        --exit-code 1 --quiet . 2>&1)
  report "trivy" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/repo -w /repo aquasec/trivy:0.74.0 fs \
        --scanners misconfig,secret --severity MEDIUM,HIGH,CRITICAL \
        --skip-dirs '**/.terraform' --exit-code 1 --quiet . 2>&1)
  report "trivy" $? "$out"
else
  skip "trivy" "needs docker or: brew install trivy"
fi

# --- Secrets ----------------------------------------------------------------
# Runs last: it is the one whose failure should be impossible to miss.
if has gitleaks; then
  out=$(gitleaks detect --source=. --no-banner --redact 2>&1); report "gitleaks" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/repo -w /repo zricethezav/gitleaks:v8.30.1 \
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

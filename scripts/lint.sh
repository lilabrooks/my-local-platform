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

# A skip is fine on a developer's machine -- they are missing a tool -- and is
# NOT fine in CI, where it means a gate silently did not run. LINT_STRICT turns
# every skip into a failure, except the names in LINT_SKIP_OK, so an intended
# exception is declared in the workflow rather than hidden in the output.
skip() {
  local name="$1" why="$2" allowed=
  case ",${LINT_SKIP_OK:-}," in *",$name,"*) allowed=1 ;; esac
  if [ -n "${LINT_STRICT:-}" ] && [ -z "$allowed" ]; then
    printf '  %s  %s -- %s\n' "$(red FAIL)" "$name" \
      "$why (LINT_STRICT: a linter that did not run is not a linter that passed)"
    FAIL=$((FAIL + 1)); FAILED+=("$name")
    return
  fi
  printf '  %s  %s -- %s\n' "$(amber SKIP)" "$name" "$why"; SKIP=$((SKIP + 1))
}

skip_allowed() {
  case ",${LINT_SKIP_OK:-}," in *",$1,"*) return 0 ;; *) return 1 ;; esac
}

# retry_net <attempts> <command...> -- for steps that reach the network.
#
# Three CI failures in a row were network, not code: a Docker Hub 500 pulling a
# base image, and tflint's plugin download from the GitHub API. Neither says
# anything about this repository, and both cost a human a re-run.
retry_net() {
  local attempts="$1"; shift
  local i out
  for i in $(seq 1 "$attempts"); do
    if out=$("$@" 2>&1); then printf '%s' "$out"; return 0; fi
    [ "$i" -lt "$attempts" ] && sleep $((i * 5))
  done
  printf '%s' "$out"
  return 1
}

has()        { command -v "$1" >/dev/null 2>&1; }
has_docker() { docker info >/dev/null 2>&1; }

echo "linting $(pwd)"
echo

# --- YAML -------------------------------------------------------------------
# Pinned, and a locally installed tool is used ONLY when it reports the pinned
# version. Preferring whatever happens to be on PATH is how this script and CI
# came to run different shellchecks: `make lint` passed 10/10 while CI failed on
# the same commit, because the two versions disagreed about SC2015 (cb8dec3).
#
# YAMLLINT_VERSION is what the image below actually contains -- checked, not
# assumed -- so the native path and the container path cannot diverge. 1.38.0 is
# published but no container ships it, and keeping a Docker-only machine able to
# run this matters more than the patch difference.
YAMLLINT_IMAGE=pipelinecomponents/yamllint:0.35.10
YAMLLINT_VERSION=1.37.1
MARKDOWNLINT_VERSION=0.23.2
SHELLCHECK_VERSION=0.11.0
ACTIONLINT_VERSION=1.7.12
HADOLINT_VERSION=2.15.1
GOLANGCI_VERSION=2.13.1
TRIVY_VERSION=0.74.0
GITLEAKS_VERSION=8.30.1

# pinned <version> <version-output> -- true when the installed tool matches.
# The numeric boundaries matter: a substring check would accept 2.13.10 when
# the repository pins 2.13.1.
pinned() {
  local escaped
  escaped=$(printf '%s' "$1" | sed 's/\./\\./g')
  printf '%s' "$2" | grep -Eq "(^|[^0-9])${escaped}([^0-9]|$)"
}

if has yamllint && pinned "$YAMLLINT_VERSION" "$(yamllint --version 2>&1)"; then
  out=$(yamllint -f parsable . 2>&1); report "yamllint" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/data -w /data "$YAMLLINT_IMAGE" \
        yamllint -f parsable . 2>&1); report "yamllint" $? "$out"
else
  skip "yamllint" "install with: pipx install yamllint==$YAMLLINT_VERSION"
fi

# --- Shell ------------------------------------------------------------------
# Not `mapfile`: macOS ships bash 3.2, which does not have it.
SCRIPTS=()
while IFS= read -r f; do SCRIPTS+=("$f"); done < <(
  find . -name '*.sh' -not -path '*/.terraform/*' | sort
)
if has shellcheck && pinned "$SHELLCHECK_VERSION" "$(shellcheck --version 2>&1)"; then
  out=$(shellcheck "${SCRIPTS[@]}" 2>&1); report "shellcheck" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/mnt -w /mnt "koalaman/shellcheck:v$SHELLCHECK_VERSION" \
        "${SCRIPTS[@]}" 2>&1); report "shellcheck" $? "$out"
else
  skip "shellcheck" "needs docker or shellcheck $SHELLCHECK_VERSION"
fi

# --- Markdown ---------------------------------------------------------------
if has markdownlint-cli2 && \
   pinned "$MARKDOWNLINT_VERSION" "$(markdownlint-cli2 --version 2>&1 | head -1)"; then
  out=$(markdownlint-cli2 2>&1); report "markdownlint" $? "$out"
elif has_docker; then
  # Prefer the container to npx. A damaged host npm cache used to fail this
  # path even though the pinned image was already available.
  out=$(retry_net 3 docker run --rm -v "$PWD":/workdir \
        "davidanson/markdownlint-cli2:v$MARKDOWNLINT_VERSION"); code=$?
  report "markdownlint" "$code" "$out"
elif has npx; then
  # @VERSION, not bare: `npx --yes markdownlint-cli2` fetches whatever is
  # newest, so this gate could change under a repository that did not.
  out=$(npx --yes "markdownlint-cli2@$MARKDOWNLINT_VERSION" 2>&1 | grep -vE '^npm notice'); code=$?
  report "markdownlint" "$code" "$out"
else
  skip "markdownlint" "needs docker, npx, or markdownlint-cli2 $MARKDOWNLINT_VERSION"
fi

# --- GitHub Actions ---------------------------------------------------------
if has actionlint && pinned "$ACTIONLINT_VERSION" "$(actionlint -version 2>&1)"; then
  out=$(actionlint 2>&1); report "actionlint" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/repo -w /repo "rhysd/actionlint:$ACTIONLINT_VERSION" 2>&1)
  report "actionlint" $? "$out"
else
  skip "actionlint" "needs docker or actionlint $ACTIONLINT_VERSION"
fi

# --- Dockerfiles ------------------------------------------------------------
DOCKERFILES=()
while IFS= read -r f; do DOCKERFILES+=("$f"); done < <(
  find . -name Dockerfile -not -path '*/.terraform/*' | sort
)
if has hadolint && pinned "$HADOLINT_VERSION" "$(hadolint --version 2>&1)"; then
  out=$(hadolint "${DOCKERFILES[@]}" 2>&1); report "hadolint" $? "$out"
elif has_docker; then
  docker_fail=0 docker_out=""
  for dockerfile in "${DOCKERFILES[@]}"; do
    out=$(docker run --rm -i "hadolint/hadolint:v$HADOLINT_VERSION-alpine" \
          hadolint - < "$dockerfile" 2>&1) || {
      docker_fail=1
      docker_out="${docker_out}${dockerfile}:
${out}
"
    }
  done
  report "hadolint" "$docker_fail" "$docker_out"
else
  skip "hadolint" "needs docker or hadolint $HADOLINT_VERSION"
fi

# --- Terraform --------------------------------------------------------------
# Pinned, with a container fallback -- the divergence here ran the other way.
# CI checked formatting with a pinned hashicorp/terraform image while this
# script used whatever `terraform` was on PATH, or skipped entirely when there
# was none. Two versions of `fmt` disagree about formatting, which is the whole
# check.
TERRAFORM_VERSION=1.15.8
tf_fail=0 tf_out=""
if has terraform && pinned "$TERRAFORM_VERSION" "$(terraform version 2>&1 | head -1)"; then
  out=$(terraform fmt -check -recursive infra/terraform 2>&1) || {
    tf_fail=1; tf_out="not formatted:\n$out"
  }
  report "terraform fmt" "$tf_fail" "$tf_out"
elif has_docker; then
  out=$(retry_net 3 docker run --rm -v "$PWD":/data -w /data \
        "hashicorp/terraform:$TERRAFORM_VERSION" fmt -check -recursive infra/terraform) || {
    tf_fail=1; tf_out="not formatted:\n$out"
  }
  report "terraform fmt" "$tf_fail" "$tf_out"
else
  skip "terraform fmt" "needs docker or terraform $TERRAFORM_VERSION"
fi

if has_docker; then
  # Cache the downloaded ruleset between runs. Without this every run refetches
  # the plugin from the GitHub API, which is slow and rate-limited.
  TFLINT_CACHE="${TMPDIR:-/tmp}/mlp-tflint-plugins"
  mkdir -p "$TFLINT_CACHE"

  lint_fail=0 lint_out=""
  for stack in infra/terraform/bootstrap infra/terraform/envs/dev; do
    # --init is NOT silenced. Hiding it once turned a GitHub 504 into an empty
    # failure with no cause, which cost more time than the flake itself -- and
    # then CI's own copy of this step silenced it again and did exactly that.
    #
    # Retried, because the plugin comes from the GitHub API: rate-limited,
    # occasionally 5xx, and nothing to do with the Terraform being linted.
    out=$(retry_net 3 docker run --rm \
      -v "$PWD":/data -v "$TFLINT_CACHE":/root/.tflint.d \
      -w "/data/$stack" \
      -e TFLINT_CONFIG_FILE=/data/.tflint.hcl \
      --entrypoint sh ghcr.io/terraform-linters/tflint:v0.64.0 \
      -c 'tflint --init && tflint --format compact') || {
        lint_fail=1
        lint_out="${lint_out}${stack}:
${out}
"
      }
  done

  # A plugin download failure is the network, not the code. Say so, because the
  # fix is "retry", not "edit terraform".
  # Allowed to skip even under LINT_STRICT, unlike a missing tool: this is a
  # DIAGNOSED network fault after three attempts, and a gate that goes red on
  # GitHub API rate limits is the flakiness this is meant to remove. The skip is
  # loud in the summary, so it cannot pass unnoticed.
  if [ "$lint_fail" -ne 0 ] && printf '%s' "$lint_out" | grep -q 'Failed to fetch GitHub releases'; then
    LINT_SKIP_OK="${LINT_SKIP_OK:-},tflint" \
      skip "tflint" "plugin download failed 3x (GitHub API); retry when it recovers"
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
go_fail=0 go_out=""
go_mods=$(find . -name go.mod -not -path './*/.terraform/*' -not -path './*/node_modules/*' \
          -exec dirname {} \; | sed 's|^\./||' | sort)
if skip_allowed "golangci-lint"; then
  skip "golangci-lint" "covered by the dedicated CI job"
elif has golangci-lint && \
   pinned "$GOLANGCI_VERSION" "$(golangci-lint --version 2>&1)"; then
  for mod in $go_mods; do
    out=$(cd "$mod" && golangci-lint run --config ../../.golangci.yml --timeout 5m 2>&1) || {
      go_fail=1
      go_out="${go_out}${mod}:
${out}
"
    }
  done
  report "golangci-lint" "$go_fail" "$go_out"
elif has_docker; then
  for mod in $go_mods; do
    out=$(docker run --rm -v "$PWD":/repo -w "/repo/$mod" \
          "golangci/golangci-lint:v$GOLANGCI_VERSION" \
          golangci-lint run --config /repo/.golangci.yml --timeout 5m 2>&1) || {
      go_fail=1
      go_out="${go_out}${mod}:
${out}
"
    }
  done
  report "golangci-lint" "$go_fail" "$go_out"
else
  skip "golangci-lint" "needs docker or golangci-lint $GOLANGCI_VERSION"
fi

# --- Infrastructure security ------------------------------------------------
# tflint checks that Terraform is valid; trivy checks whether it is safe. They
# overlap not at all -- trivy found six issues tflint passed clean.
# --skip-dirs matters: .terraform/ holds vendored upstream modules whose
# example manifests are not ours to fix.
TRIVY_CACHE="${TMPDIR:-/tmp}/mlp-trivy-cache"
TRIVY_CHECKS_INPUT="$TRIVY_CACHE/checks-prefetch-input"
mkdir -p "$TRIVY_CACHE" "$TRIVY_CHECKS_INPUT"
if has trivy && pinned "$TRIVY_VERSION" "$(trivy --version 2>&1 | head -1)"; then
  if db_out=$(retry_net 3 trivy image --download-db-only \
      --cache-dir "$TRIVY_CACHE"); then
    if checks_out=$(retry_net 3 trivy config --cache-dir "$TRIVY_CACHE" \
        --exit-code 0 --quiet "$TRIVY_CHECKS_INPUT"); then
      out=$(trivy fs --scanners vuln,misconfig,secret \
            --cache-dir "$TRIVY_CACHE" --skip-db-update --skip-check-update \
            --severity MEDIUM,HIGH,CRITICAL \
            --skip-dirs '**/.terraform' \
            --exit-code 1 --quiet . 2>&1)
      report "trivy" $? "$out"
    else
      report "trivy" 1 "checks bundle download failed after 3 attempts:\n$checks_out"
    fi
  else
    report "trivy" 1 "vulnerability database download failed after 3 attempts:\n$db_out"
  fi
elif has_docker; then
  if db_out=$(retry_net 3 docker run --rm --user "$(id -u):$(id -g)" \
      -v "$TRIVY_CACHE":/trivy-cache "aquasec/trivy:$TRIVY_VERSION" \
      image --download-db-only --cache-dir /trivy-cache); then
    if checks_out=$(retry_net 3 docker run --rm --user "$(id -u):$(id -g)" \
        -v "$TRIVY_CACHE":/trivy-cache "aquasec/trivy:$TRIVY_VERSION" \
        config --cache-dir /trivy-cache --exit-code 0 --quiet \
        /trivy-cache/checks-prefetch-input); then
      out=$(docker run --rm --user "$(id -u):$(id -g)" \
            -v "$PWD":/repo -v "$TRIVY_CACHE":/trivy-cache \
            -w /repo "aquasec/trivy:$TRIVY_VERSION" fs --cache-dir /trivy-cache \
            --skip-db-update --skip-check-update --scanners vuln,misconfig,secret \
            --severity MEDIUM,HIGH,CRITICAL --skip-dirs '**/.terraform' \
            --exit-code 1 --quiet . 2>&1)
      report "trivy" $? "$out"
    else
      report "trivy" 1 "checks bundle download failed after 3 attempts:\n$checks_out"
    fi
  else
    report "trivy" 1 "vulnerability database download failed after 3 attempts:\n$db_out"
  fi
else
  skip "trivy" "needs docker or trivy $TRIVY_VERSION"
fi

# --- Secrets ----------------------------------------------------------------
# Runs last: it is the one whose failure should be impossible to miss.
if has gitleaks && pinned "$GITLEAKS_VERSION" "$(gitleaks version 2>&1)"; then
  out=$(gitleaks detect --source=. --no-banner --redact 2>&1); report "gitleaks" $? "$out"
elif has_docker; then
  out=$(docker run --rm -v "$PWD":/repo -w /repo "zricethezav/gitleaks:v$GITLEAKS_VERSION" \
        detect --source=. --no-banner --redact 2>&1); report "gitleaks" $? "$out"
else
  skip "gitleaks" "needs docker or gitleaks $GITLEAKS_VERSION"
fi

echo
printf 'passed %d, failed %d, skipped %d\n' "$PASS" "$FAIL" "$SKIP"
if [ "$FAIL" -gt 0 ]; then
  printf 'failing: %s\n' "${FAILED[*]}"
  exit 1
fi

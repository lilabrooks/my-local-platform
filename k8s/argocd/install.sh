#!/usr/bin/env bash
# Install ArgoCD into the current cluster and register the app-of-apps.
#
# Idempotent. Safe to re-run after changing REPO_URL.
set -euo pipefail

# The AppProject boundary below depends on the cluster-resource name filter
# added in ArgoCD 3.3. Keep this fixed, rather than allowing an environment
# override to install an older CRD that accepts the field but cannot enforce it.
readonly ARGOCD_VERSION="v3.5.1"
REPO_URL="${REPO_URL:-https://github.com/lilabrooks/my-local-platform.git}"
NAMESPACE=argocd

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_APP="${ROOT_APPLICATION_FILE:-$HERE/root-app.yaml}"

[ -f "$ROOT_APP" ] || { echo "root Application not found: $ROOT_APP" >&2; exit 2; }
if [ -n "${ROOT_APPLICATION_FILE:-}" ]; then
  root_repo=$(python3 -c \
    'import json, sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["spec"]["source"]["repoURL"])' \
    "$ROOT_APP")
  if [ "$root_repo" != "$REPO_URL" ]; then
    echo "generated root repository $root_repo does not match REPO_URL $REPO_URL" >&2
    exit 2
  fi
fi

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

# Deploying to whatever context happens to be current is how test workloads end
# up in production clusters. Show it and require agreement.
CONTEXT=$(kubectl config current-context)
say "target context: $CONTEXT"
if [ "${ASSUME_YES:-}" != "1" ]; then
  read -r -p "    install ArgoCD $ARGOCD_VERSION here? [y/N] " ok
  [ "$ok" = "y" ] || { echo "aborted"; exit 1; }
fi

say "creating namespace $NAMESPACE"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

say "installing ArgoCD $ARGOCD_VERSION"
# --server-side is required, not a preference. Client-side apply stores the
# whole manifest in a last-applied-configuration annotation, and the
# ApplicationSet CRD alone exceeds the 262144-byte annotation limit:
#   The CustomResourceDefinition "applicationsets.argoproj.io" is invalid:
#   metadata.annotations: Too long
kubectl apply -n "$NAMESPACE" --server-side --force-conflicts \
  -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml" >/dev/null

say "waiting for ArgoCD to become available (this takes a couple of minutes on first run)"
for deploy in argocd-repo-server argocd-server argocd-applicationset-controller argocd-dex-server argocd-redis; do
  kubectl rollout status -n "$NAMESPACE" "deployment/$deploy" --timeout=300s
done
kubectl rollout status -n "$NAMESPACE" statefulset/argocd-application-controller --timeout=300s

say "applying scoped AppProjects and root Application (repo: $REPO_URL)"
# The manifests carry a placeholder so a fork can point them elsewhere without
# editing tracked files. This order keeps an existing root app valid while its
# project moves, then narrows the workload and default projects.
for f in \
  "$HERE/root-project.yaml" \
  "$ROOT_APP" \
  "$HERE/project.yaml" \
  "$HERE/default-project.yaml"; do
  sed "s|__REPO_URL__|${REPO_URL}|g" "$f" | kubectl apply -f - >/dev/null
done

say "ArgoCD ready"
echo
echo "    UI       kubectl port-forward -n argocd svc/argocd-server 8081:443"
echo "             then https://localhost:8081  (self-signed cert warning is expected)"
echo "    user     admin"
echo "    password kubectl -n argocd get secret argocd-initial-admin-secret \\"
echo "               -o jsonpath='{.data.password}' | base64 -d"
echo
echo "    NOTE: ArgoCD pulls manifests from git, not from your working tree."
echo "    Until $REPO_URL exists and has these files pushed on 'main',"
echo "    the applications will report a repository error. That is expected."

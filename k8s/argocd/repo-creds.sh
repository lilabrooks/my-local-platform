#!/usr/bin/env bash
# Give ArgoCD read access to this private repository.
#
# ArgoCD clones over the network, so a private repo needs credentials. This
# creates a read-only SSH deploy key scoped to this one repository -- narrower
# than a personal access token, which would carry `repo` scope across every
# repo you own.
#
#   ./k8s/argocd/repo-creds.sh
#
# The private key goes into an ArgoCD Secret in the cluster and a local file
# under ~/.ssh. Neither is ever written into the repository.
set -euo pipefail

REPO_SLUG="${REPO_SLUG:-lilabrooks/my-local-platform}"
KEY_PATH="${KEY_PATH:-$HOME/.ssh/argocd_${REPO_SLUG##*/}}"
NAMESPACE=argocd

say() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

command -v gh >/dev/null || { echo "gh is required" >&2; exit 1; }

if [ ! -f "$KEY_PATH" ]; then
  say "generating a read-only deploy key at $KEY_PATH"
  ssh-keygen -t ed25519 -N "" -C "argocd@${REPO_SLUG}" -f "$KEY_PATH" >/dev/null
else
  say "reusing existing key at $KEY_PATH"
fi

# --read-only matters: ArgoCD only ever needs to pull. A writable key in a
# cluster is a way for a compromised workload to rewrite your git history.
if gh repo deploy-key list --repo "$REPO_SLUG" 2>/dev/null | grep -q "argocd-${REPO_SLUG##*/}"; then
  say "deploy key already present on $REPO_SLUG"
else
  say "adding the public key to $REPO_SLUG as read-only"
  gh repo deploy-key add "${KEY_PATH}.pub" \
    --repo "$REPO_SLUG" --title "argocd-${REPO_SLUG##*/}" --read-only
fi

say "creating the ArgoCD repository Secret"
kubectl create secret generic "repo-${REPO_SLUG##*/}" \
  --namespace "$NAMESPACE" \
  --from-literal=type=git \
  --from-literal=url="git@github.com:${REPO_SLUG}.git" \
  --from-file=sshPrivateKey="$KEY_PATH" \
  --dry-run=client -o yaml \
  | kubectl label -f - --local --dry-run=client -o yaml \
      argocd.argoproj.io/secret-type=repository \
  | kubectl apply -f - >/dev/null

say "pointing the Applications at the SSH URL"
SSH_URL="git@github.com:${REPO_SLUG}.git"
for f in k8s/argocd/project.yaml k8s/argocd/root-app.yaml; do
  sed "s|__REPO_URL__|${SSH_URL}|g" "$f" | kubectl apply -f - >/dev/null
done

say "done -- forcing a refresh"
kubectl patch application root -n "$NAMESPACE" --type merge \
  -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}' >/dev/null

echo
echo "    watch it sync:  kubectl get applications -n argocd -w"

#!/usr/bin/env bash
set -euo pipefail

# Kubeconform validates the built-in Kubernetes objects without a cluster.
# Custom-resource schemas are covered by the focused Go invariants because the
# ArgoCD, KEDA, and ServiceMonitor CRDs are installed only in a live cluster.
readonly KUBECONFORM_IMAGE="ghcr.io/yannh/kubeconform:v0.8.0-alpine@sha256:6b90a5f23d846140ce0194fe050b1995e546eba938f3a6bf10c039dd5e24588f"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ROOT
kps_version="$(awk '$1 == "KPS_VERSION" && $2 == "?=" { print $3 }' "$ROOT/Makefile")"
[ -n "$kps_version" ] || { echo "could not read KPS_VERSION from Makefile" >&2; exit 1; }
readonly kps_version
scratch="$(mktemp -d)"
bundle="$scratch/manifests.yaml"
trap 'rm -rf "$scratch"' EXIT INT TERM

render() {
  {
    kubectl kustomize "$1"
    printf '\n---\n'
  } >>"$bundle"
}

rendered=0
while IFS= read -r kustomization; do
  [ -n "$kustomization" ] || continue
  render "$(dirname "$kustomization")"
  rendered=$((rendered + 1))
done < <(find \
  "$ROOT/k8s/manifests" \
  "$ROOT/k8s/aws" \
  "$ROOT/k8s/apps/aws" \
  -name kustomization.yaml -type f -print | sort)
[ "$rendered" -gt 0 ] || { echo "no Kubernetes render roots discovered" >&2; exit 1; }

{
  cat "$ROOT/k8s/aws/runtime-configmap.example.yaml"
  printf '\n---\n'
  cat "$ROOT/k8s/aws/runtime-secret.example.yaml"
  printf '\n---\n'
  python3 "$ROOT/scripts/render-aws-k8s.py" replay \
    --commit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    --relay-image 123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/relay@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  printf '\n---\n'
  python3 "$ROOT/scripts/render-aws-k8s.py" runtime \
    --msk-bootstrap boot-example.c1.kafka-serverless.us-east-1.amazonaws.com:9098
  printf '\n---\n'
  python3 "$ROOT/scripts/render-aws-k8s.py" application \
    --commit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    --relay-image 123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/relay@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    --sink-image 123456789012.dkr.ecr.us-east-1.amazonaws.com/mlp-dev/sink@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
    --msk-bootstrap boot-example.c1.kafka-serverless.us-east-1.amazonaws.com:9098
  printf '\n---\n'
  HELM_CACHE_HOME="$scratch/helm-cache" \
  HELM_CONFIG_HOME="$scratch/helm-config" \
  HELM_DATA_HOME="$scratch/helm-data" \
    helm template monitoring kube-prometheus-stack \
    --repo https://prometheus-community.github.io/helm-charts \
    --version "$kps_version" \
    --namespace monitoring \
    --values "$ROOT/k8s/monitoring-values.yaml" \
    --values "$ROOT/k8s/monitoring-values-aws.yaml"
} >>"$bundle"

docker run --rm -i "$KUBECONFORM_IMAGE" \
  -strict \
  -summary \
  -ignore-missing-schemas \
  -kubernetes-version 1.35.0 \
  - <"$bundle"

# Kubernetes schema validation cannot inspect YAML embedded in ConfigMaps.
# Ask the exact binaries used by the Deployments to parse those files too.
docker run --rm --read-only --user 10001:10001 \
  -v "$ROOT/k8s/aws/telemetry/config/tempo.yaml:/etc/tempo.yaml:ro" \
  grafana/tempo:3.0.3 \
  -config.file=/etc/tempo.yaml -config.verify=true
docker run --rm --read-only --user 10001:10001 \
  -v "$ROOT/k8s/aws/telemetry/config/collector.yaml:/etc/otel.yaml:ro" \
  otel/opentelemetry-collector-contrib:0.159.0 \
  validate --config=/etc/otel.yaml

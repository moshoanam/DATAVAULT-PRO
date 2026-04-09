#!/usr/bin/env bash
# deploy.sh — local Jib build + K8s deploy helper
# Usage:
#   ./scripts/deploy.sh tar              # build OCI tar (no registry)
#   ./scripts/deploy.sh push <tag>       # build & push to registry
#   ./scripts/deploy.sh k8s <overlay>    # kubectl apply overlay (dev|staging|production)
#   ./scripts/deploy.sh all <tag>        # push + deploy production
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VERSION=$(grep '^version' "$PROJECT_ROOT/build.gradle" | head -1 | grep -oP "'[^']+'" | tr -d "'")
REGISTRY="${REGISTRY:-registry.gitlab.com/yourorg/datavault-pro/backend}"

cd "$PROJECT_ROOT"

build_tar() {
  echo "==> Building OCI tar with Jib (no registry required)..."
  export JAVA_HOME="${JAVA_HOME:-$HOME/jdk17/Contents/Home}"
  export PATH="$JAVA_HOME/bin:/tmp/gradle/gradle-8.5/bin:$PATH"
  gradle jibBuildTar
  echo "==> Image tar: build/jib-image.tar"
  echo "    Load with: docker load < build/jib-image.tar"
  echo "    Or:        podman load < build/jib-image.tar"
}

push_image() {
  local tag="${1:-$VERSION}"
  echo "==> Building & pushing image :${tag} with Jib..."
  export JAVA_HOME="${JAVA_HOME:-$HOME/jdk17/Contents/Home}"
  export PATH="$JAVA_HOME/bin:/tmp/gradle/gradle-8.5/bin:$PATH"
  IMAGE_TAG="$tag" gradle jib
  echo "==> Pushed: ${REGISTRY}:${tag}"
}

deploy_k8s() {
  local overlay="${1:-dev}"
  echo "==> Applying Kustomize overlay: k8s/overlays/${overlay}"
  kubectl kustomize "k8s/overlays/${overlay}" | kubectl apply -f -
  local ns="datavault"
  [[ "$overlay" == "dev" ]] && ns="datavault-dev"
  kubectl -n "$ns" rollout status deployment/datavault-backend --timeout=5m
  echo "==> Deploy complete (${overlay})"
}

case "${1:-help}" in
  tar)      build_tar ;;
  push)     push_image "${2:-$VERSION}" ;;
  k8s)      deploy_k8s "${2:-dev}" ;;
  all)
    push_image "${2:-$VERSION}"
    deploy_k8s "production"
    ;;
  *)
    echo "Usage: $0 {tar|push [tag]|k8s [overlay]|all [tag]}"
    exit 1
    ;;
esac

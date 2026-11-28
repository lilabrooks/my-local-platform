#!/usr/bin/env bash
# Package Lambda binaries locally. 'hourly' also prepares the private MySQL
# fixture and a matching executor ZIP. Nothing in this file contacts AWS.
set -euo pipefail
umask 077
root=$(cd "$(dirname "$0")/../../.." && pwd)
mode=${1:-identity}
out=${2:?supply a new private output directory}
[[ $mode == identity || $mode == hourly ]]
mkdir -m 700 "$out"
out=$(cd "$out" && pwd)
tmp=$(mktemp -d "$out/.build.XXXXXX")
stage_tag=
cleanup() {
  if [[ -n $stage_tag ]]; then docker image rm "$stage_tag" >/dev/null; fi
  rm -rf "$tmp"
}
trap cleanup EXIT
cd "$tmp"
package() {
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go -C "$root/services/db-mcp" build -tags lambda.norpc -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o "$tmp/bootstrap" "./cmd/db-mcp-$1"
  chmod 0755 bootstrap
  TZ=UTC touch -t 198001020000 bootstrap
  shift
  TZ=UTC zip -Xq "$zip_path" bootstrap "$@"
}
zip_path="$out/router.zip"
package router
if [[ $mode == hourly ]]; then
  compose_image=$(sed -n 's/^    image: \(mysql:[^ ]*\)$/\1/p' "$root/local/docker-compose.db-mcp.yml")
  [[ $compose_image == mysql:*@sha256:* ]]
  docker pull --platform linux/amd64 "$compose_image" >/dev/null
  # Classic Docker and containerd can report different IDs for the same
  # image. A tag in the archive survives import on either image store.
  source_id=$(docker image inspect --platform linux/amd64 --format '{{.Id}}' "$compose_image")
  stage_tag="mlp-db-mcp-fixture:$(openssl rand -hex 16)"
  docker image tag "$source_id" "$stage_tag"
  printf '%s\n' "$stage_tag" >image-tag
  docker image save --platform linux/amd64 -o mysql.tar "$stage_tag"
  tar -xOf mysql.tar manifest.json | jq -e --arg tag "$stage_tag" 'length == 1 and .[0].RepoTags == [$tag]' >/dev/null
  bash "$root/services/db-mcp/pilot/certificates.sh"
  chmod 0644 mysql-ca.pem
  TZ=UTC touch -t 198001020000 mysql-ca.pem
  zip_path="$out/executor.zip"
  package executor mysql-ca.pem
  cp "$root/local/bootstrap/db-mcp/mysql/10-meter.sql" "$root/services/db-mcp/pilot/mysql-container.sh" .
  tar -cf "$out/fixture.tar" mysql.tar image-tag mysql-ca.pem server-cert.pem server-key.pem 10-meter.sql mysql-container.sh
fi
cd "$out"
if [[ $mode == hourly ]]; then
  shasum -a 256 router.zip executor.zip fixture.tar
else
  shasum -a 256 router.zip
fi

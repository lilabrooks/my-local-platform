#!/usr/bin/env bash
# Run the packaged Linux MySQL fixture on a random loopback port.
set -euo pipefail
umask 077
root=$(cd "$(dirname "$0")/../../.." && pwd)
work=$(mktemp -d)
work=$(cd "$work" && pwd -P)
name="mlp-db-mcp-test-$(openssl rand -hex 8)"
loaded_tag=
cleanup() {
  if docker inspect "$name" >/dev/null 2>&1; then docker rm -fv "$name" >/dev/null; fi
  if [[ -n $loaded_tag ]]; then docker image rm "$loaded_tag" >/dev/null; fi
  rm -rf "$work"
}
trap cleanup EXIT
if [[ $# == 0 ]]; then
  artifacts="$work/artifacts"
  bash "$root/services/db-mcp/pilot/package.sh" hourly "$artifacts"
else
  artifacts=$(cd "$1" && pwd -P)
fi
cd "$work"
tar -xf "$artifacts/fixture.tar"
unzip -p "$artifacts/executor.zip" mysql-ca.pem | cmp - mysql-ca.pem
openssl verify -CAfile mysql-ca.pem server-cert.pem >/dev/null
openssl x509 -in mysql-ca.pem -noout -checkend 7200 >/dev/null
openssl x509 -in server-cert.pem -noout -checkend 7200 >/dev/null
image=$(cat image-tag)
[[ $image =~ ^mlp-db-mcp-fixture:[a-f0-9]{32}$ ]]
if ! docker image inspect "$image" >/dev/null 2>&1; then loaded_tag=$image; fi
docker image load --input mysql.tar >/dev/null
[[ $(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image") == linux/amd64 ]]
bash mysql-container.sh "$image" "$name" '127.0.0.1::3306' "$work/credentials"
binding=$(docker port "$name" 3306/tcp)
[[ $binding == 127.0.0.1:* ]]
export PILOT_TEST_PORT=${binding##*:} PILOT_TEST_CA="$work/mysql-ca.pem" PILOT_TEST_READER="$work/credentials/reader"
mkdir -m 700 "$work/unrelated"
(cd "$work/unrelated" && bash "$root/services/db-mcp/pilot/certificates.sh")
export PILOT_TEST_OTHER_CA="$work/unrelated/mysql-ca.pem"
go -C "$root/services/db-mcp" test -race -tags=integration -count=1 -v -run '^TestPilotMySQL$' ./internal/pilot

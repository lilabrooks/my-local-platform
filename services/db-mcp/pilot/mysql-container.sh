#!/usr/bin/env bash
# Both the local TLS rehearsal and the private EC2 fixture run this script.
# The caller supplies the reviewed image ID, container name and port binding.
set -euo pipefail
umask 077
image=${1:?image}
name=${2:?name}
binding=${3:?binding}
files=$PWD
secret_dir=${4:?private credential directory}
mkdir -m 700 "$secret_dir"
openssl rand -hex 32 | tr -d '\n' >"$secret_dir/root"
openssl rand -hex 32 | tr -d '\n' >"$secret_dir/reader"
# The containing host directories are 0700. Only these individual files are
# mounted into mysqld, which drops privileges before reading them.
chmod 0444 "$secret_dir/root" "$secret_dir/reader" server-key.pem server-cert.pem mysql-ca.pem 10-meter.sql
docker run -d --platform linux/amd64 --pull=never --name "$name" -p "$binding" \
  -e MYSQL_DATABASE=meter_demo -e MYSQL_USER=meter_reader \
  -e MYSQL_PASSWORD_FILE=/run/mcp-reader -e MYSQL_ROOT_PASSWORD_FILE=/run/mcp-owner \
  -v "$secret_dir/reader:/run/mcp-reader:ro" -v "$secret_dir/root:/run/mcp-owner:ro" \
  -v "$files/10-meter.sql:/docker-entrypoint-initdb.d/10-meter.sql:ro" \
  -v "$files/mysql-ca.pem:/run/mysql-ca.pem:ro" -v "$files/server-cert.pem:/run/mysql-cert.pem:ro" \
  -v "$files/server-key.pem:/run/mysql-key.pem:ro" "$image" \
  --require-secure-transport=ON --ssl-ca=/run/mysql-ca.pem --ssl-cert=/run/mysql-cert.pem --ssl-key=/run/mysql-key.pem >/dev/null
query() {
  docker exec "$name" sh -ec 'export MYSQL_PWD="$(cat /run/mcp-reader)"; exec mysql -NB --protocol=TCP -h127.0.0.1 -umeter_reader --ssl-mode=VERIFY_IDENTITY --ssl-ca=/run/mysql-ca.pem meter_demo -e "$1"' sh "$1"
}
for ((i=0; i<90; i++)); do
  if value=$(query "SELECT COUNT(*) FROM readings WHERE state='ready'" 2>/dev/null) && [[ $value == 32 ]]; then break; fi
  [[ $(docker inspect -f '{{.State.Running}}' "$name") == true ]] || { echo "MySQL exited during startup" >&2; exit 1; }
  sleep 1
done
[[ $(query "SELECT COUNT(*) FROM readings WHERE state='ready'") == 32 ]]
[[ $(query "SHOW SESSION STATUS LIKE 'Ssl_cipher'") == *$'\t'?* ]]
for stmt in 'SELECT service_note FROM meter_events LIMIT 1' 'UPDATE readings SET watts=0 WHERE 1=0'; do
  if denial=$(query "$stmt" 2>&1) || [[ $denial != *'ERROR 1142 '* && $denial != *'ERROR 1143 '* ]]; then
    echo "Native MySQL grant did not deny the excluded operation as expected." >&2
    exit 1
  fi
done
echo "MySQL fixture verified 32 ready readings, TLS and both native grant denials."

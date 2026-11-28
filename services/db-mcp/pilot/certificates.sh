#!/usr/bin/env bash
# Called in an empty private directory by the fixture packager or local test.
set -euo pipefail
umask 077
openssl req -x509 -newkey rsa:2048 -nodes -days 7 -subj '/CN=Disposable database MCP CA' \
  -keyout issuer.key -out mysql-ca.pem >/dev/null 2>&1
openssl req -new -newkey rsa:2048 -nodes -subj '/CN=pilot-mysql' \
  -keyout server-key.pem -out server.csr >/dev/null 2>&1
cat >server.ext <<'EOF'
basicConstraints=critical,CA:FALSE
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
subjectAltName=IP:127.0.0.1,IP:10.73.0.10
EOF
openssl x509 -req -in server.csr -CA mysql-ca.pem -CAkey issuer.key -CAcreateserial -days 7 \
  -extfile server.ext -out server-cert.pem >/dev/null 2>&1
openssl verify -CAfile mysql-ca.pem server-cert.pem >/dev/null
rm -f issuer.key mysql-ca.srl server.csr server.ext

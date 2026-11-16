#!/usr/bin/env bash
# PHP ext-mongodb (libmongoc) smoke test through the proxy, in front of the 3-node replica set.
# Generates throwaway TLS certs and a proxy users file into a temp dir (never committed), then runs the
# php-smoke compose service. No host ports are published.
set -euo pipefail

cd "$(dirname "$0")/.."

PROJECT=mongobetween-test

SMOKE_CERTS_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mongobetween-smoke.XXXXXX")"
export SMOKE_CERTS_DIR
trap 'rm -rf "$SMOKE_CERTS_DIR"' EXIT

SMOKE_PROXY_PASSWORD="${SMOKE_PROXY_PASSWORD:-$(openssl rand -hex 16)}"
export SMOKE_PROXY_PASSWORD

# The proxy runs as uid 65532 (distroless nonroot), so the mounted files must be world-readable here;
# the proxy warns about a group/other-readable users file, which is acceptable for a throwaway test.
printf 'app:%s\n' "$SMOKE_PROXY_PASSWORD" > "$SMOKE_CERTS_DIR/users.txt"

# Throwaway CA + server cert (SAN mongobetween-secure) generated with openssl from the mongo image.
docker run --rm --user "$(id -u):$(id -g)" -v "$SMOKE_CERTS_DIR":/certs -w /certs --entrypoint sh mongo:8.0 -ec '
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=smoke-ca" -keyout ca.key -out ca.pem >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -subj "/CN=mongobetween-secure" -keyout server.key -out server.csr >/dev/null 2>&1
  printf "subjectAltName=DNS:mongobetween-secure\n" > san.ext
  openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 -extfile san.ext -out server.pem >/dev/null 2>&1
  rm -f server.csr san.ext ca.srl
'
chmod 0644 "$SMOKE_CERTS_DIR"/*
chmod 0755 "$SMOKE_CERTS_DIR"

docker compose -p "$PROJECT" up -d --wait rs1 rs2 rs3
docker compose -p "$PROJECT" run --rm rs-init

docker compose -p "$PROJECT" --profile php up --build --abort-on-container-exit \
  --exit-code-from php-smoke php-smoke

#!/usr/bin/env bash
# Runs the test suite inside a container on a compose network containing
# three standalone mongo:8.0 nodes and a three-member mongo:8.0 replica set.
# No host ports are published, so this never touches a MongoDB on the host.
# Usage: ARGS="-run TestName -v" ./scripts/test-docker.sh
set -euo pipefail

cd "$(dirname "$0")/.."

PROJECT=mongobetween-test

docker compose -p "$PROJECT" up -d --wait mongo1 mongo2 mongo3 rs1 rs2 rs3
docker compose -p "$PROJECT" run --rm rs-init

# ARGS is intentionally unquoted so it word-splits into go test flags.
docker run --rm --network "${PROJECT}_default" \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e GOCACHE=/tmp/gocache \
  -e CI=true \
  -e "MONGOBETWEEN_RS_URI=mongodb://rs1:27017,rs2:27017,rs3:27017/?replicaSet=rs0" \
  -v "$PWD":/src:ro \
  -v "$(go env GOMODCACHE)":/go/pkg/mod \
  -e "ARGS=${ARGS:-}" \
  golang:1.27 sh -c 'cp -r /src /tmp/work && cd /tmp/work && go test -count 1 -race ./... $ARGS'

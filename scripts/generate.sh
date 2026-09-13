#!/usr/bin/env bash
# Regenerate the typed client from the published OpenAPI document.
#
#   ./scripts/generate.sh              # fetch https://0xinsider.com/api/v1/openapi.json
#   SPEC=path/to/openapi.json ./scripts/generate.sh
#
# The bracketed query aliases (`expand[]`) are backward-compatible spellings of
# `expand`; they collide with the canonical parameter in Go names, so they are
# dropped before generation. Requests from this SDK always use `expand`.
set -euo pipefail
cd "$(dirname "$0")/.."
OAPI_CODEGEN_VERSION="v2.8.0"
spec="${SPEC:-}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
if [ -z "$spec" ]; then
  curl -fsSL -A "0xinsider-go-generator" https://0xinsider.com/api/v1/openapi.json -o "$tmp/openapi.json"
  spec="$tmp/openapi.json"
fi
jq '(.paths[][] | objects | select(has("parameters")) | .parameters) |= map(select((.name // "") | endswith("[]") | not))' \
  "$spec" > openapi.sdk.json
go run "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@${OAPI_CODEGEN_VERSION}" \
  -config scripts/oapi-codegen.yaml openapi.sdk.json
gofmt -l . | (! grep .)
go vet ./...
go build ./...

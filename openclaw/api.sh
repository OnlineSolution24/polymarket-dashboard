#!/bin/bash
# Polymarket Bot API Helper
# Usage: source /data/mcp/api.sh
# Then: api_get /api/markets "limit=5"
#        api_post /api/strategies '{"name":"test"}'

API_URL="http://172.17.0.1:8000"
API_KEY="Bk9cYmaMari8zDC6uOOJ_m9by-hnji8HpuM7p4h4g74"

api_get() {
  local path="$1"
  local params="$2"
  local url="${API_URL}${path}"
  if [ -n "$params" ]; then url="${url}?${params}"; fi
  curl -s -H "Authorization: Bearer ${API_KEY}" "$url"
}

api_post() {
  local path="$1"
  local body="$2"
  curl -s -X POST -H "Authorization: Bearer ${API_KEY}" -H "Content-Type: application/json" -d "$body" "${API_URL}${path}"
}

api_put() {
  local path="$1"
  local body="$2"
  curl -s -X PUT -H "Authorization: Bearer ${API_KEY}" -H "Content-Type: application/json" -d "$body" "${API_URL}${path}"
}

api_delete() {
  local path="$1"
  curl -s -X DELETE -H "Authorization: Bearer ${API_KEY}" "${API_URL}${path}"
}

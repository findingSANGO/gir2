#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./track_ticket_enrich.sh ticket_enrich_.... [interval_seconds]
#
# Example:
#   ./track_ticket_enrich.sh ticket_enrich_20260103T124125Z_d95c8204 5

RUN_ID="${1:-}"
INTERVAL="${2:-5}"

if [[ -z "${RUN_ID}" ]]; then
  echo "ERROR: RUN_ID is required."
  echo "Usage: $0 <run_id> [interval_seconds]"
  exit 1
fi

API="http://localhost:8000"

TOKEN="$(
  curl -fsS -X POST -H 'Content-Type: application/json' \
    -d '{"username":"commissioner","password":"commissioner123"}' \
    "${API}/api/auth/login" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])"
)"

while true; do
  J="$(curl -fsS -H "Authorization: Bearer ${TOKEN}" "${API}/api/data/runs/${RUN_ID}" || true)"
  if [[ -z "${J}" ]]; then
    echo "waiting for run..."
    sleep "${INTERVAL}"
    continue
  fi

  python3 - <<PY
import json, sys, datetime
r=json.loads(sys.argv[1])
run_status=r.get("status")
total=int(r.get("total_rows") or 0)
processed=int(r.get("processed") or 0)
skipped=int(r.get("skipped") or 0)
failed=int(r.get("failed") or 0)
done_n=processed+skipped+failed
pending=max(0, total-done_n)
ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"{ts} status={run_status} total={total} processed={processed} skipped={skipped} failed={failed} pending={pending}")
PY "${J}"

  RUN_STATUS="$(python3 -c "import sys,json; print(json.loads(sys.argv[1]).get('status') or '')" "${J}")"
  if [[ "${RUN_STATUS}" == "completed" || "${RUN_STATUS}" == "failed" ]]; then
    break
  fi
  sleep "${INTERVAL}"
done



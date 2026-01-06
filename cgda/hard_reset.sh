#!/usr/bin/env bash
set -euo pipefail

# HARD RESET: deletes ALL derived data (SQLite DB + processed exports + run artifacts + logs).
# Does NOT delete raw inputs under data/raw or data/raw2.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

stop_pidfile() {
  local pidfile="$1"
  if [[ ! -f "$pidfile" ]]; then
    return 0
  fi
  local pid
  pid="$(tr -d '\r\n ' < "$pidfile" || true)"
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "Stopping $pidfile (pid=$pid)"
    kill "$pid" 2>/dev/null || true
    sleep 1
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$pidfile" || true
}

echo "== Stopping dev processes (best effort) =="
stop_pidfile "backend.dev.pid"
stop_pidfile "frontend.dev.pid"
stop_pidfile "cloudflared_quicktunnel.pid"
stop_pidfile "frontend/frontend.dev.pid"

echo "== Deleting derived DBs =="
rm -f "backend/cgda.db" || true
rm -f "data/processed/cgda.db" || true

echo "== Deleting derived exports + run artifacts =="
rm -f data/processed/* || true
rm -f data/runs/* || true
rm -f data/preprocess/* || true
rm -f data/stage_data/* || true
rm -f data/ai_outputs/* || true
rm -f data/outputs/* || true

echo "== Deleting logs =="
rm -f backend.dev.log frontend.dev.log cloudflared_quicktunnel.log || true

echo ""
echo "Hard reset complete."
echo "Raw inputs preserved:"
echo "  - data/raw/"
echo "  - data/raw2/"



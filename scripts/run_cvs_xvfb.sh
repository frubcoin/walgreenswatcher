#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CRAWLEE_DIR="$REPO_ROOT/backend/crawlee"

if ! command -v xvfb-run >/dev/null 2>&1; then
  echo "xvfb-run is required but was not found on PATH." >&2
  echo "Install xvfb first, for example: sudo apt-get install -y xvfb" >&2
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "node is required but was not found on PATH." >&2
  exit 1
fi

export CVS_XVFB_HEADLESS="${CVS_XVFB_HEADLESS:-0}"
export DISPLAY="${DISPLAY:-}"

cd "$CRAWLEE_DIR"

exec xvfb-run -a \
  --server-args="-screen 0 1920x1080x24 -ac +extension RANDR" \
  node cvs-xvfb-test.mjs "$@"

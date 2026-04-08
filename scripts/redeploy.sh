#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
SERVICE_NAMES_RAW="${SERVICE_NAMES:-${SERVICE_NAME:-gunicorn walgreenswatcher}}"
APP_USER="${APP_USER:-walgreens}"
VENV_PATH="${VENV_PATH:-}"

log() {
  printf '[redeploy] %s\n' "$*"
}

fail() {
  printf '[redeploy] ERROR: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

trim() {
  local value="${1:-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

detect_app_user() {
  local detected=""
  local primary_service_name=""

  primary_service_name="${SERVICE_NAMES_RAW%% *}"

  if [[ -n "${APP_USER}" ]]; then
    printf '%s' "$APP_USER"
    return
  fi

  if require_cmd stat 2>/dev/null; then
    detected="$(stat -c '%U' "$APP_DIR" 2>/dev/null || true)"
    detected="$(trim "$detected")"
    if [[ -n "$detected" && "$detected" != "UNKNOWN" && "$detected" != "root" ]]; then
      printf '%s' "$detected"
      return
    fi
  fi

  if command -v systemctl >/dev/null 2>&1; then
    detected="$(systemctl show "$primary_service_name" --property=User --value 2>/dev/null || true)"
    detected="$(trim "$detected")"
    if [[ -n "$detected" && "$detected" != "root" ]]; then
      printf '%s' "$detected"
      return
    fi
  fi

  if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
    printf '%s' "$SUDO_USER"
    return
  fi

  id -un
}

run_as_app_user() {
  local target_user="$1"
  shift

  if [[ "$(id -un)" == "$target_user" ]]; then
    "$@"
    return
  fi

  if command -v sudo >/dev/null 2>&1; then
    sudo -H -u "$target_user" "$@"
    return
  fi

  if command -v runuser >/dev/null 2>&1; then
    runuser -u "$target_user" -- "$@"
    return
  fi

  fail "Need sudo or runuser to execute commands as ${target_user}"
}

cleanup_python_artifacts() {
  local target_user="${1:-}"

  log "Cleaning Python cache artifacts"
  if [[ -n "$target_user" && "$(id -un)" != "$target_user" ]]; then
    run_as_app_user "$target_user" find "$APP_DIR" \
      \( -path "$APP_DIR/.git" -o -path "$APP_DIR/.venv" -o -path "$APP_DIR/node_modules" -o -path "$APP_DIR/backend/crawlee/node_modules" \) -prune \
      -o \( -type d -name "__pycache__" -print -exec rm -rf {} + \) \
      -o \( -type f \( -name "*.pyc" -o -name "*.pyo" \) -print -delete \)
    return
  fi

  find "$APP_DIR" \
    \( -path "$APP_DIR/.git" -o -path "$APP_DIR/.venv" -o -path "$APP_DIR/node_modules" -o -path "$APP_DIR/backend/crawlee/node_modules" \) -prune \
    -o \( -type d -name "__pycache__" -print -exec rm -rf {} + \) \
    -o \( -type f \( -name "*.pyc" -o -name "*.pyo" \) -print -delete \)
}

find_python() {
  if [[ -n "$VENV_PATH" && -x "$VENV_PATH/bin/python" ]]; then
    printf '%s' "$VENV_PATH/bin/python"
    return
  fi

  if [[ -x "$APP_DIR/.venv/bin/python" ]]; then
    printf '%s' "$APP_DIR/.venv/bin/python"
    return
  fi

  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return
  fi

  if command -v python >/dev/null 2>&1; then
    command -v python
    return
  fi

  fail "Could not find a Python interpreter"
}

find_browser() {
  local candidate=""
  local candidates=(
    "${CVS_ZENDRIVER_BROWSER_EXECUTABLE_PATH:-}"
    "/usr/bin/google-chrome"
    "/usr/bin/google-chrome-stable"
    "/usr/bin/chromium"
    "/usr/bin/chromium-browser"
    "/usr/bin/microsoft-edge"
    "/snap/bin/chromium"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      printf '%s' "$candidate"
      return
    fi
  done

  if command -v google-chrome >/dev/null 2>&1; then
    command -v google-chrome
    return
  fi
  if command -v google-chrome-stable >/dev/null 2>&1; then
    command -v google-chrome-stable
    return
  fi
  if command -v chromium >/dev/null 2>&1; then
    command -v chromium
    return
  fi
  if command -v chromium-browser >/dev/null 2>&1; then
    command -v chromium-browser
    return
  fi
  if command -v microsoft-edge >/dev/null 2>&1; then
    command -v microsoft-edge
    return
  fi

  return 1
}

main() {
  require_cmd git

  [[ -d "$APP_DIR/.git" ]] || fail "APP_DIR does not look like a git repo: $APP_DIR"

  local target_user
  target_user="$(detect_app_user)"
  [[ -n "$target_user" ]] || fail "Could not determine app user"

  local -a service_names=()
  read -r -a service_names <<<"$SERVICE_NAMES_RAW"
  [[ ${#service_names[@]} -gt 0 ]] || fail "No systemd service names configured"

  log "App dir: $APP_DIR"
  log "App user: $target_user"
  log "Services: ${service_names[*]}"

  git config --global --add safe.directory "$APP_DIR" >/dev/null 2>&1 || true
  run_as_app_user "$target_user" git config --global --add safe.directory "$APP_DIR" >/dev/null 2>&1 || true

  cleanup_python_artifacts "$target_user"

  log "Fetching latest refs"
  run_as_app_user "$target_user" git -C "$APP_DIR" fetch --prune

  log "Pulling latest code"
  run_as_app_user "$target_user" git -C "$APP_DIR" pull --ff-only

  cleanup_python_artifacts "$target_user"

  local python_bin
  python_bin="$(find_python)"
  log "Python: $python_bin"

  log "Installing Python dependencies"
  run_as_app_user "$target_user" "$python_bin" -m pip install -r "$APP_DIR/backend/requirements.txt"

  log "Verifying zendriver import"
  run_as_app_user "$target_user" "$python_bin" -c "import zendriver; print(zendriver.__version__)"

  local browser_bin=""
  if browser_bin="$(find_browser)"; then
    log "Browser: $browser_bin"
  else
    log "Browser not auto-detected. Set CVS_ZENDRIVER_BROWSER_EXECUTABLE_PATH if needed."
  fi

  if command -v systemctl >/dev/null 2>&1; then
    log "Reloading systemd units"
    systemctl daemon-reload
    for service_name in "${service_names[@]}"; do
      systemctl reset-failed "$service_name" >/dev/null 2>&1 || true
      log "Restarting ${service_name}"
      systemctl restart "$service_name"
      systemctl --no-pager --full status "$service_name" || true
      log "Recent ${service_name} logs"
      journalctl -u "$service_name" -n 30 --no-pager || true
    done
  else
    log "systemctl not found, skipping service restart"
  fi
}

main "$@"

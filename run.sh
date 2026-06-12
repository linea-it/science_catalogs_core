#!/bin/bash
set -Eeuo pipefail
set -o errtrace

ENV_NAME="pipe_sc"
MICROMAMBA_BIN="${MICROMAMBA_BIN:-${MAMBA_EXE:-micromamba}}"
MAMBA_ROOT_PREFIX="${MAMBA_ROOT_PREFIX:-$HOME/.micromamba}"

resolve_micromamba() {
  local resolved

  if resolved="$(command -v "$MICROMAMBA_BIN" 2>/dev/null)" && [ -n "$resolved" ]; then
    printf '%s\n' "$resolved"
    return 0
  fi

  return 1
}

log() {
  local ts
  ts="$(date "+%Y-%m-%d %H:%M:%S")"
  echo "[$ts] $*"
}

trap '{
  code=$?
  log "❌ Fail (exit code: ${code})"
  exit $code
}' ERR

if [ $# -lt 1 ]; then
  echo "Usage: ./run.sh <config.yaml> [run_dir]"
  exit 1
fi

CONFIG_PATH="$1"
RUN_DIR="${2:-process001}"
mkdir -p "$RUN_DIR"

PIPE_BASE="$(cd "$(dirname "$0")" && pwd)"

log "Ensuring the installed environment..."
bash "${PIPE_BASE}/install.sh"

if ! MICROMAMBA_BIN="$(resolve_micromamba)"; then
  echo "❌ micromamba not found"
  echo "   Add micromamba to PATH or set MICROMAMBA_BIN=/path/to/micromamba."
  exit 1
fi

LOGS_DIR="$RUN_DIR/process_info"
mkdir -p "$LOGS_DIR"
LOG_FILE="$LOGS_DIR/process.log"

exec > >(tee -a "$LOG_FILE") 2>&1

log "Running pipeline..."

"$MICROMAMBA_BIN" run --root-prefix "$MAMBA_ROOT_PREFIX" -n "$ENV_NAME" bash -c "
  export PATH=${PIPE_BASE}/scripts:\$PATH
  export PYTHONPATH=${PIPE_BASE}/packages:\$PYTHONPATH
  exec sc-run \"\$1\" \"\$2\"
" bash "$CONFIG_PATH" "$RUN_DIR"

log "✅ Success (run dir: ${RUN_DIR})"
exit 0

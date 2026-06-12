#!/bin/bash
set -Eeuo pipefail

SCRIPT_PATH=$(readlink -f "$0")
PIPELINE_DIR=$(dirname "$SCRIPT_PATH")

ENV_NAME="pipe_sc"
ENV_FILE="$PIPELINE_DIR/environment.yaml"
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

if ! MICROMAMBA_BIN="$(resolve_micromamba)"; then
  echo "❌ micromamba not found"
  echo "   Add micromamba to PATH or set MICROMAMBA_BIN=/path/to/micromamba."
  exit 1
fi

log() {
  local ts
  ts="$(date "+%Y-%m-%d %H:%M:%S")"
  echo "[$ts] $*"
}

if [ ! -f "$ENV_FILE" ]; then
  echo "❌ ${ENV_FILE} not found"
  exit 1
fi

ENV_HASH=$(sha256sum "$ENV_FILE" | awk '{print $1}')
HASH_FILE="$PIPELINE_DIR/.env_hash"

log "Hash do ambiente: $ENV_HASH"

env_exists() {
  "$MICROMAMBA_BIN" env list --root-prefix "$MAMBA_ROOT_PREFIX" | awk '{print $1}' | grep -q "^${ENV_NAME}$"
}

if env_exists; then
  log "Environment '${ENV_NAME}' already exists."
  if [ -f "$HASH_FILE" ]; then
    EXISTING_HASH=$(cat "$HASH_FILE")
    if [ "$EXISTING_HASH" = "$ENV_HASH" ]; then
      log "✅ Environment is up to date."
      exit 0
    else
      log "⚠️ environment.yaml changed. Recreating environment..."
      "$MICROMAMBA_BIN" env remove --root-prefix "$MAMBA_ROOT_PREFIX" -n "$ENV_NAME" -y
    fi
  else
    log "⚠️ Environment hash not found. Recreating..."
    "$MICROMAMBA_BIN" env remove --root-prefix "$MAMBA_ROOT_PREFIX" -n "$ENV_NAME" -y
  fi
fi

log "📦 Creating environment '${ENV_NAME}'..."
"$MICROMAMBA_BIN" create --root-prefix "$MAMBA_ROOT_PREFIX" -y -n "$ENV_NAME" -f "$ENV_FILE"
echo "$ENV_HASH" > "$HASH_FILE"

log "✅ Environment ready."

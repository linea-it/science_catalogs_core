#!/bin/bash
set -Eeuo pipefail

SCRIPT_PATH=$(readlink -f "$0")
PIPELINE_DIR=$(dirname "$SCRIPT_PATH")

ENV_NAME="pipe_sc"
ENV_FILE="$PIPELINE_DIR/environment.yaml"
MICROMAMBA_BIN="${MICROMAMBA_BIN:-micromamba}"
MAMBA_ROOT_PREFIX="${MAMBA_ROOT_PREFIX:-$HOME/.micromamba}"

log() {
  local ts
  ts="$(date "+%Y-%m-%d %H:%M:%S")"
  echo "[$ts] $*"
}

if ! command -v "$MICROMAMBA_BIN" >/dev/null 2>&1; then
  echo "❌ micromamba não encontrado no PATH"
  exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
  echo "❌ ${ENV_FILE} não encontrado"
  exit 1
fi

ENV_HASH=$(sha256sum "$ENV_FILE" | awk '{print $1}')
HASH_FILE="$PIPELINE_DIR/.env_hash"

log "Hash do ambiente: $ENV_HASH"

env_exists() {
  "$MICROMAMBA_BIN" env list --root-prefix "$MAMBA_ROOT_PREFIX" | awk '{print $1}' | grep -q "^${ENV_NAME}$"
}

if env_exists; then
  log "Ambiente '${ENV_NAME}' já existe."
  if [ -f "$HASH_FILE" ]; then
    EXISTING_HASH=$(cat "$HASH_FILE")
    if [ "$EXISTING_HASH" = "$ENV_HASH" ]; then
      log "✅ Ambiente está atualizado."
      exit 0
    else
      log "⚠️ environment.yaml mudou. Recriando ambiente..."
      "$MICROMAMBA_BIN" env remove --root-prefix "$MAMBA_ROOT_PREFIX" -n "$ENV_NAME" -y
    fi
  else
    log "⚠️ Hash do ambiente não encontrado. Recriando..."
    "$MICROMAMBA_BIN" env remove --root-prefix "$MAMBA_ROOT_PREFIX" -n "$ENV_NAME" -y
  fi
fi

log "📦 Criando ambiente '${ENV_NAME}'..."
"$MICROMAMBA_BIN" create --root-prefix "$MAMBA_ROOT_PREFIX" -y -n "$ENV_NAME" -f "$ENV_FILE"
echo "$ENV_HASH" > "$HASH_FILE"

log "✅ Ambiente instalado."

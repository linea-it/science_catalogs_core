#!/bin/bash
set -euo pipefail

export APP_DIR=$(pwd)
if [ -z "${DATASETS_DIR:-}" ]; then
  export DATASETS_DIR="$APP_DIR/data-example"
fi

echo "APP_DIR=$APP_DIR"
echo "DATASETS_DIR=$DATASETS_DIR"
read -p "Enter 'yes' para confirmar a geração de config.yaml/env.sh com esses caminhos: " resp
if [ "$resp" != "yes" ]; then
  echo "Abortado. Ajuste DATASETS_DIR e rode novamente."
  exit 1
fi

# Gera config.yaml a partir do template
sed "s|<DATASETS_DIR>|$DATASETS_DIR|g; s|<APP_DIR>|$APP_DIR|g" \
  "$APP_DIR/config.template.yaml" > "$APP_DIR/config.yaml"

echo "config.yaml gerado."

# Gera env.sh
sed "s|<APP_DIR>|$APP_DIR|g; s|<DATASETS_DIR>|$DATASETS_DIR|g" \
  "$APP_DIR/env.template.sh" > "$APP_DIR/env.sh"
chmod +x "$APP_DIR/env.sh"

echo "env.sh gerado."

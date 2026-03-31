#!/bin/bash
set -euo pipefail

export APP_DIR=$(pwd)
if [ -z "${DATASETS_DIR:-}" ]; then
  export DATASETS_DIR="$APP_DIR/data-example"
fi

echo "APP_DIR=$APP_DIR"
echo "DATASETS_DIR=$DATASETS_DIR"
read -p "Enter 'yes' to generate config.yaml/env.sh with these paths: " resp
if [ "$resp" != "yes" ]; then
  echo "Aborted. Adjust DATASETS_DIR and rerun."
  exit 1
fi

# Gera config.yaml a partir do template
sed "s|<DATASETS_DIR>|$DATASETS_DIR|g; s|<APP_DIR>|$APP_DIR|g" \
  "$APP_DIR/config.template.yaml" > "$APP_DIR/config.yaml"

echo "config.yaml generated."

# Gera env.sh
sed "s|<APP_DIR>|$APP_DIR|g; s|<DATASETS_DIR>|$DATASETS_DIR|g" \
  "$APP_DIR/env.template.sh" > "$APP_DIR/env.sh"
chmod +x "$APP_DIR/env.sh"

echo "env.sh generated."

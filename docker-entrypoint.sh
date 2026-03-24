#!/bin/sh
set -eu

EXTERNAL_CONFIG_PATH="${STOCK_DB_CONFIG_MOUNT_PATH:-/config/config.toml}"
APP_CONFIG_PATH="/app/backend/app/config/config.toml"

if [ -f "$EXTERNAL_CONFIG_PATH" ]; then
    cp "$EXTERNAL_CONFIG_PATH" "$APP_CONFIG_PATH"
    echo "Loaded external database config from $EXTERNAL_CONFIG_PATH"
fi

exec "$@"

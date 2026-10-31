#!/usr/bin/env bash
# Démarrage de Melodi WebSocket

PORT="${1:-${PORT:-3002}}"
PLATFORM="${PLATFORM:-linux}"
SECRET="${2:-${SECRET:-melodi_ws_secret_456}}"
MELODI_PORT="${3:-${MELODI_PORT:-3000}}"
MELODI_HOST="${4:-${MELODI_HOST:-localhost}}"
MELODI_PLATFORM="${5:-${MELODI_PLATFORM:-linux}}"

if [ -f "dist/melodi_websocket" ]; then
    echo "Démarrage de l'exécutable compilé melodi_websocket..."
    exec ./dist/melodi_websocket --port "$PORT" --secret "$SECRET" --melodi-port "$MELODI_PORT" --melodi-host "$MELODI_HOST" --melodi-platform "$MELODI_PLATFORM" "$@"
else
    if [ -d "venv" ]; then
        source venv/bin/activate
    fi
    exec python3 main.py --port "$PORT" --secret "$SECRET" --melodi-port "$MELODI_PORT" --melodi-host "$MELODI_HOST" --melodi-platform "$MELODI_PLATFORM" "$@"
fi

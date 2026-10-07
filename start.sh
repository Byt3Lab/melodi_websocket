#!/usr/bin/env bash
# Démarrage de Melodi WebSocket

PORT="${1:-8002}"
SECRET="${2:-melodi_ws_secret_456}"
MELODI_PORT="${3:-3000}"

if [ -f "dist/melodi_websocket" ]; then
    echo "Démarrage de l'exécutable compilé..."
    exec ./dist/melodi_websocket --port "$PORT" --secret "$SECRET" --melodi-port "$MELODI_PORT"
else
    if [ -d "venv" ]; then
        source venv/bin/activate
    fi
    exec python3 main.py --port "$PORT" --secret "$SECRET" --melodi-port "$MELODI_PORT"
fi

#!/usr/bin/env bash
# Démarrage de Melodi WebSocket

PORT="${1:-9000}"
PLATFORM="${PLATFORM:-linux}"
SECRET="${2:-melodi_ws_secret_456}"
MELODI_PORT="${3:-8000}"
MELODI_HOST="${4:-localhost}"
MELODI_PLAfTORM="${5:-linux}"

if [ -d "venv" ]; then
    source venv/bin/activate
fi
exec python3 main.py --port "$PORT" --secret "$SECRET" --melodi-port "$MELODI_PORT" --melodi-host "$MELODI_HOST" --melodi-platform "$MELODI_PLAfTORM"    

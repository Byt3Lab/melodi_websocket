#!/usr/bin/env bash
set -e

echo "=== Compilation de Melodi WebSocket (PyInstaller) ==="

# Activer venv si présent
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Nettoyage
rm -rf build dist

# Compilation
pyinstaller --clean melodi_websocket.spec

chmod +x dist/melodi_websocket 2>/dev/null || true

echo "=== Compilation terminée ! Exécutable dans dist/melodi_websocket ==="

#!/usr/bin/env bash
set -e

echo "=== Compilation de Melodi WebSocket (Go) ==="

# Nettoyage
mkdir -p dist

# Compilation binaire statique optimisé
go build -ldflags="-s -w" -o dist/melodi_websocket cmd/server/main.go

chmod +x dist/melodi_websocket

echo "=== Compilation terminée ! Exécutable autonome dans dist/melodi_websocket ==="

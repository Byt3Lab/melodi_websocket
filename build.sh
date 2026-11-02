#!/usr/bin/env bash
set -e

echo "=== Compilation de Melodi WebSocket (Go) ==="

# Nettoyage et création du dossier de distribution
mkdir -p dist

# Compilation binaire statique autonome CGO=0
CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/melodi_websocket cmd/server/main.go

chmod +x dist/melodi_websocket

echo "=== Compilation terminée ! Exécutable autonome dans dist/melodi_websocket ==="

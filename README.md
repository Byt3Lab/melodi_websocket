# Melodi WebSocket

Service autonome de WebSocket et de messagerie temps réel pour l'écosystème **MELODI ERP**.

## Fonctionnalités

- Gestion des connexions WebSocket pour les clients web et API (`/ws`).
- Canaux de diffusion (`broadcast`) et messages ciblés par utilisateur (`user`).
- Endpoint interne REST (`/internal/broadcast`) sécurisé par clé secrète partagée pour l'émission d'événements depuis MELODI.
- Endpoints de statut et de santé (`/health`, `/metrics`, `/status`).
- Indépendant et découplé du core de MELODI.
- Compilable en exécutable autonome avec PyInstaller.

## Prérequis et Installation

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Lancement

En mode développement :
```bash
./start.sh [PORT] [SECRET] [MELODI_PORT]
# ou
python3 main.py --port 8002 --secret melodi_ws_secret_456 --melodi-port 3000
```

## Compilation PyInstaller

Pour générer l'exécutable autonome dans `dist/melodi_websocket` :

```bash
chmod +x build.sh
./build.sh
# ou
python3 build.py
```

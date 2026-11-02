# Melodi WebSocket

Service autonome de WebSocket et de messagerie temps réel pour l'écosystème **MELODI ERP**.

## Fonctionnalités

- Gestion des connexions WebSocket pour les clients web et API (`/ws`).
- Canaux de diffusion (`broadcast`) et messages ciblés par utilisateur (`user`).
- Endpoint interne REST (`/internal/broadcast`) sécurisé par clé secrète partagée pour l'émission d'événements depuis MELODI.
- Endpoints de statut et de santé (`/health`, `/internal/health`).
- **Administration complète à distance depuis Melodi ERP** (`/internal/config`, `/internal/stats`, `/internal/admin/*`).
- **Persistance automatique** des configurations dans `storage/config.json`.
- **Application dynamique à chaud** (période de grâce, niveau de log, clé secrète, basculement PubSub autonome/scalable).
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

---

## API d'Administration pour Melodi ERP

Tous les endpoints sous `/internal/` sont sécurisés et accessibles uniquement :
1. Depuis localhost ou les sous-réseaux IP privés (Docker, LAN).
2. Avec la clé secrète transmise via le header `X-Internal-Secret: <secret>` ou `Authorization: Bearer <secret>`.

### 1. Gestion des Configurations (`/internal/config`)

#### Consulter la configuration active
```http
GET /internal/config
GET /internal/config?include_secrets=true
```
- Par défaut, les secrets (`secret`, `redis_password`) sont masqués (`******` ou format tronqué `mel...456`).
- Pour obtenir les valeurs en clair dans l'interface ERP, spécifier `?include_secrets=true`.

**Exemple de réponse JSON :**
```json
{
  "status": "ok",
  "node_id": "node_7a9f82d1",
  "uptime_seconds": 124.5,
  "scale_mode": "autonome",
  "active_connections": 12,
  "active_channels": 3,
  "config": {
    "server": {
      "host": "0.0.0.0",
      "port": 8002,
      "env": "dev",
      "log_level": "INFO"
    },
    "security": {
      "secret": "mel...456",
      "secret_configured": true,
      "allow_private_internal": true
    },
    "integration": {
      "melodi_host": "localhost",
      "melodi_port": 3000,
      "melodi_platform": "linux"
    },
    "pubsub": {
      "scale_mode": "autonome",
      "redis_host": "localhost",
      "redis_port": 6379,
      "redis_password_configured": false,
      "redis_db": 0,
      "redis_channel": "melodi:ws:events"
    },
    "sessions": {
      "grace_period_seconds": 180,
      "max_connections": 10000,
      "ping_interval_seconds": 30
    },
    "persistence": {
      "config_file": "/app/storage/config.json",
      "file_exists": true
    }
  }
}
```

#### Modifier la configuration depuis l'ERP
```http
POST /internal/config
Content-Type: application/json
```
```json
{
  "grace_period_seconds": 120,
  "log_level": "DEBUG",
  "scale_mode": "scalable",
  "redis_host": "127.0.0.1",
  "redis_port": 6379,
  "redis_password": "mon_mot_de_passe",
  "persist": true
}
```
- **Prise d'effet immédiate (sans redémarrage)** : `grace_period_seconds`, `log_level`, `secret`, et basculement dynamique du bus PubSub (`scale_mode`, `redis_*`).
- **Nécessite un redémarrage** : modification de `port` ou `host`. Le champ `restart_required: true` sera renvoyé dans la réponse.

#### Tester une configuration sans l'appliquer
```http
POST /internal/config/test
Content-Type: application/json
```
```json
{
  "target": "redis",
  "redis_host": "localhost",
  "redis_port": 6379,
  "redis_password": ""
}
```
Réponse :
```json
{
  "status": "ok",
  "success": true,
  "target": "redis",
  "latency_ms": 1.45,
  "message": "Redis connection successful on localhost:6379 (PONG)"
}
```

#### Réinitialiser la configuration
```http
POST /internal/config/reset
```

---

### 2. Métriques et Supervision (`/internal/stats`)

```http
GET /internal/stats
```
Fournit un bilan complet en temps réel :
- Uptime, PID, mémoire RSS consommée.
- Nombre de sockets actifs et total accepté depuis le démarrage.
- Total des messages WebSocket reçus et envoyés.
- Liste des canaux actifs et nombre d'abonnés par canal.
- Utilisateurs connectés uniques (`online_user_ids`).
- Sessions en attente de reconnexion dans la période de grâce.

---

### 3. Gestion des Canaux et Clients (`/internal/admin/*`)

- **Lister les canaux actifs** :
  ```http
  GET /internal/admin/channels
  ```

- **Lister les clients connectés** (filtrage optionnel par `?channel=` ou `?user_id=`) :
  ```http
  GET /internal/admin/clients
  GET /internal/admin/clients?channel=orders
  ```

- **Déconnecter / Expulser un client ou un utilisateur** :
  ```http
  POST /internal/admin/clients/disconnect
  Content-Type: application/json

  {
    "client_id": "client_13982348",
    "reason": "Déconnexion demandée par l'administrateur"
  }
  ```
  ou
  ```json
  {
    "user_id": "42",
    "reason": "Compte suspendu"
  }
  ```

- **Purger les sessions en période de grâce** :
  ```http
  POST /internal/admin/sessions/clear-grace
  ```

- **Recharger la configuration depuis le disque** :
  ```http
  POST /internal/admin/reload
  ```

- **Déclencher un redémarrage propre** :
  ```http
  POST /internal/admin/restart
  ```

---

## Compilation PyInstaller

Pour générer l'exécutable autonome dans `dist/melodi_websocket` :

```bash
chmod +x build.sh
./build.sh
# ou
python3 build.py
```

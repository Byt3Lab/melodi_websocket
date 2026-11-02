# Melodi WebSocket (Version Go)

Service autonome de WebSocket et de messagerie temps réel haute performance pour l'écosystème **MELODI ERP**, développé nativement en **Go**.

> **Note :** Cette branche contient l'implémentation 100% Go de `melodi_websocket` (zéro dépendance Python).

---

## Fonctionnalités

- **Performances & Concurrence Go** : Gestion asynchrone non-bloquante de dizaines de milliers de connexions WebSocket concurrentes via des goroutines légères.
- **WebSocket Hub & Routage** : Support des canaux de diffusion (`broadcast`) et messages ciblés par utilisateur (`user`).
- **Reprise de Session & Grâce** : Préservation des sessions et réabonnement automatique lors des reconnexions transitoires (`grace_period_seconds`).
- **Pub/Sub Distribué (Cluster Redis)** :
  - **Mode Autonome** : Pub/sub local en mémoire pour serveur unique.
  - **Mode Scalable** : Cluster Redis Pub/Sub multi-nœuds avec détection et validation obligatoire de la connexion Redis au démarrage.
- **Administration & Télémétrie ERP** : Endpoints REST `/internal/*` sécurisés par clé secrète pour configuration à chaud, rechargement et statistiques en temps réel.
- **Conteneurisation Minimale** : Image Docker multi-stage Alpine de ~15 Mo.

---

## Prérequis

- **Go** : 1.22 ou supérieur
- *(Optionnel pour mode cluster)* : **Redis** 7+

---

## Installation & Démarrage

### 1. En développement (avec Go)

```bash
# Lancement direct avec go run
go run cmd/server/main.go --port 8070 --secret melodi_ws_secret_456

# Ou via le script start.sh
./start.sh 8070 melodi_ws_secret_456 3000
```

### 2. Compilation de l'exécutable autonome

```bash
# Génère le binaire statique dans dist/melodi_websocket
./build.sh

# Lancement du binaire autonome
./dist/melodi_websocket --port 8070 --secret melodi_ws_secret_456
```

### 3. Exécution avec Docker

```bash
# Construction de l'image
docker build -t melodi_websocket:go .

# Démarrage
docker run -d -p 8070:8070 --name melodi_ws melodi_websocket:go
```

---

## Tests

La suite de tests unitaires et d'intégration est exécutée via les outils natifs de Go :

```bash
# Exécution de tous les tests
go test -v ./...

# Avec rapport de couverture
go test -cover ./...
```

---

## API & Endpoints

### 1. Endpoints Publics
- `GET /` & `GET /health` : Vérification de l'état de santé du service.
- `GET /ws` : Endpoint de négociation et connexion WebSocket.

### 2. Endpoints Internes Sécurisés (`/internal/*`)
*Protégés par filtrage IP et header `X-Internal-Secret: <secret>` ou `Authorization: Bearer <secret>`.*

- `POST /internal/broadcast` : Diffuse un événement à un ou plusieurs canaux ou utilisateurs.
- `GET /internal/health` : Santé détaillée avec état Redis et statistiques mémoire Go (goroutines, heap alloc).
- `GET /internal/stats` : Métriques de trafic, sessions actives, canaux et uptime.
- `GET /internal/config` : Lecture de la configuration active (masquée par défaut, en clair avec `?include_secrets=true`).
- `POST /internal/config` : Mise à jour à chaud de la configuration (période de grâce, niveau de log, Redis, persistance dans `storage/config.json`).
- `GET /internal/sessions` : Liste des sessions WebSocket et abonnements en mémoire.
- `POST /internal/admin/restart` : Demande d'arrêt / redémarrage contrôlé par MELODI ERP.

---

## Structure du Projet

```
.
├── cmd/
│   └── server/
│       └── main.go           # Point d'entrée principal du serveur
├── internal/
│   ├── api/                  # Routeur chi, handlers HTTP et WebSocket, tests
│   ├── auth/                 # Contrôle d'accès, secrets et filtrage d'adresses IP
│   ├── config/               # Gestionnaire de configuration dynamique et persistance
│   ├── hub/                  # Gestionnaire de connexions, clients, sessions et grâce
│   └── pubsub/               # Abstraction Pub/Sub (Local & Redis Cluster)
├── build.sh                  # Script de compilation binaire statique Go
├── start.sh                  # Script de démarrage rapide Go
├── Dockerfile                # Build multi-stage optimisé (Go builder -> Alpine)
├── docker-compose.yml        # Orchestration locale
├── go.mod                    # Définition des modules et dépendances Go
└── go.sum                    # Sommes de contrôle des dépendances
```


import os
import sys
import time
import json
import uuid
import asyncio
import logging
import argparse
import ipaddress
from typing import Dict, Set, Any
from quart import Quart, request, jsonify, websocket
from pubsub import PubSubBackend, LocalPubSub, RedisPubSub
from config_manager import ConfigManager, mask_secret

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("melodi_websocket")

app = Quart(__name__)

# Node identifier for cluster tracing
node_id = f"node_{uuid.uuid4().hex[:8]}"

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Melodi WebSocket Real-Time Service")
parser.add_argument("--secret", type=str, default=None, help="Internal shared secret key")
parser.add_argument("--port", type=int, default=None, help="WebSocket service port")
parser.add_argument("--melodi-port", type=int, default=None, help="Main Melodi application port")
parser.add_argument("--melodi-host", type=str, default=None, help="Main Melodi application host")
parser.add_argument("--melodi-platform", type=str, default=None, help="Main Melodi application platform")
parser.add_argument("--env", type=str, default=None, help="Environment")
parser.add_argument("--scale-mode", type=str, default=None, choices=["autonome", "scalable", "standalone", "cluster"], help="Scale mode: autonome or scalable")
parser.add_argument("--redis-url", type=str, default=None, help="Redis URL for scalable mode (e.g. redis://:password@host:6379/0)")
parser.add_argument("--redis-host", type=str, default=None, help="Redis host")
parser.add_argument("--redis-port", type=int, default=None, help="Redis port")
parser.add_argument("--redis-password", type=str, default=None, help="Redis password")
parser.add_argument("--redis-db", type=int, default=None, help="Redis database index")
parser.add_argument("--redis-channel", type=str, default=None, help="Redis pub/sub channel name")
args, unknown = parser.parse_known_args()

# Initialize configuration manager
config_mgr = ConfigManager()
config = config_mgr.init_config(cli_args=args)
config["node_id"] = node_id

# Set configured log level
log_level_name = config.get("log_level", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.getLogger().setLevel(log_level)
logger.setLevel(log_level)

scale_mode = config.get("scale_mode", "autonome")

# Initialize Pub/Sub Backend
if scale_mode == "scalable":
    pubsub: PubSubBackend = RedisPubSub(
        redis_url=config.get("redis_url"),
        host=config.get("redis_host", "localhost"),
        port=config.get("redis_port", 6379),
        password=config.get("redis_password"),
        db=config.get("redis_db", 0),
        channel=config.get("redis_channel", "melodi:ws:events")
    )
else:
    pubsub: PubSubBackend = LocalPubSub()


class ConnectionManager:
    """
    Registry and fanout manager for real-time WebSocket connections.
    Tracks active connections, user mappings, channel subscriptions,
    maintains a grace period for reconnecting sessions, and exposes
    administrative inspection and termination capabilities.
    """
    def __init__(self, grace_period_seconds: int = 180):
        # socket object -> client metadata dictionary
        self.active_connections: Dict[Any, Dict[str, Any]] = {}
        # user_id -> Set of socket objects
        self.user_sockets: Dict[str, Set[Any]] = {}
        # channel_name -> Set of socket objects
        self.channel_sockets: Dict[str, Set[Any]] = {}
        # session_id -> { user_id, channels, disconnect_task, last_active }
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.grace_period_seconds = grace_period_seconds

        # Metrics counters
        self.total_connections_accepted = 0
        self.total_messages_received = 0
        self.total_messages_sent = 0

    def register(self, ws, user_id: str | None = None, session_id: str | None = None, remote_ip: str | None = None) -> str:
        client_id = f"client_{id(ws)}"
        user_str = str(user_id) if user_id else None
        now_ts = time.time()

        self.active_connections[ws] = {
            "client_id": client_id,
            "user_id": user_str,
            "session_id": session_id,
            "channels": set(),
            "connected_at": now_ts,
            "remote_ip": remote_ip
        }
        self.total_connections_accepted += 1

        if user_str:
            if user_str not in self.user_sockets:
                self.user_sockets[user_str] = set()
            self.user_sockets[user_str].add(ws)

        # Handle session resumption within grace period
        if session_id:
            if session_id in self.sessions:
                session_data = self.sessions[session_id]
                disconnect_task = session_data.get("disconnect_task")
                if disconnect_task and not disconnect_task.done():
                    disconnect_task.cancel()
                    session_data["disconnect_task"] = None
                    logger.info(f"[Melodi WebSocket] Session {session_id} resumed within {self.grace_period_seconds}s grace period.")

                # Restore previous subscriptions
                for ch in list(session_data.get("channels", set())):
                    self.subscribe(ws, ch)

                if user_str:
                    session_data["user_id"] = user_str
            else:
                self.sessions[session_id] = {
                    "user_id": user_str,
                    "channels": set(),
                    "disconnect_task": None
                }

        logger.info(f"[Melodi WebSocket] Client registered: {client_id} (user_id: {user_id}, session_id: {session_id}, ip: {remote_ip})")
        return client_id

    def subscribe(self, ws, channel: str):
        if ws in self.active_connections:
            self.active_connections[ws]["channels"].add(channel)
            session_id = self.active_connections[ws].get("session_id")
            if session_id and session_id in self.sessions:
                self.sessions[session_id]["channels"].add(channel)

            if channel not in self.channel_sockets:
                self.channel_sockets[channel] = set()
            self.channel_sockets[channel].add(ws)
            logger.info(f"[Melodi WebSocket] Client {self.active_connections[ws]['client_id']} subscribed to '{channel}'")

    def unsubscribe(self, ws, channel: str):
        if ws in self.active_connections:
            self.active_connections[ws]["channels"].discard(channel)
            session_id = self.active_connections[ws].get("session_id")
            if session_id and session_id in self.sessions:
                self.sessions[session_id]["channels"].discard(channel)

            if channel in self.channel_sockets:
                self.channel_sockets[channel].discard(ws)

    def unregister(self, ws):
        if ws in self.active_connections:
            info = self.active_connections.pop(ws)
            client_id = info.get("client_id")
            user_id = info.get("user_id")
            session_id = info.get("session_id")

            if user_id and user_id in self.user_sockets:
                self.user_sockets[user_id].discard(ws)
                if not self.user_sockets[user_id]:
                    del self.user_sockets[user_id]

            for channel in info.get("channels", set()):
                if channel in self.channel_sockets:
                    self.channel_sockets[channel].discard(ws)
                    if not self.channel_sockets[channel]:
                        del self.channel_sockets[channel]

            # Start grace period before permanently removing the session
            if session_id and session_id in self.sessions:
                session_data = self.sessions[session_id]
                session_data["channels"] = info.get("channels", set())

                async def _expire_session_task(sid: str, uid: str | None):
                    try:
                        await asyncio.sleep(self.grace_period_seconds)
                        if sid in self.sessions:
                            del self.sessions[sid]
                            logger.info(f"[Melodi WebSocket] Session {sid} (user: {uid}) expirée après {self.grace_period_seconds}s. Déconnexion définitive.")
                            if uid and uid not in self.user_sockets:
                                # Broadcast offline status to presence channels
                                envelope = {
                                    "origin_node": node_id,
                                    "message": {
                                        "event": "user_presence",
                                        "data": {"user_id": uid, "status": "offline"}
                                    },
                                    "channels": ["presence", "chat:presence"]
                                }
                                await pubsub.publish(envelope)
                    except asyncio.CancelledError:
                        pass

                task = asyncio.create_task(_expire_session_task(session_id, user_id))
                session_data["disconnect_task"] = task
                logger.info(f"[Melodi WebSocket] Déconnexion socket {client_id}. Période de grâce de {self.grace_period_seconds}s activée pour session {session_id}.")
            else:
                logger.info(f"[Melodi WebSocket] Client unregistered: {client_id}")

    async def _safe_send(self, ws, message: dict):
        try:
            await ws.send(json.dumps(message))
            self.total_messages_sent += 1
        except Exception as e:
            logger.warning(f"[Melodi WebSocket] Send error to socket: {e}")

    async def local_broadcast(self, message: dict, user_ids: list | None = None, channels: list | None = None) -> int:
        """
        Broadcasts message to locally connected clients (matching user_ids or channels, or all).
        """
        target_sockets = set()

        if not user_ids and not channels:
            target_sockets = set(self.active_connections.keys())
        else:
            if user_ids:
                for uid in user_ids:
                    sockets = self.user_sockets.get(str(uid), set())
                    target_sockets.update(sockets)

            if channels:
                for ch in channels:
                    sockets = self.channel_sockets.get(ch, set())
                    target_sockets.update(sockets)

        if not target_sockets:
            return 0

        tasks = [self._safe_send(ws, message) for ws in target_sockets]
        await asyncio.gather(*tasks, return_exceptions=True)
        return len(target_sockets)

    # Alias for backward compatibility
    broadcast = local_broadcast

    # Administrative inspection & management methods
    def get_clients(self, channel: str | None = None, user_id: str | None = None) -> list[dict]:
        """Returns list of active clients with metadata, optionally filtered."""
        now_ts = time.time()
        clients = []
        for ws, info in self.active_connections.items():
            if channel and channel not in info.get("channels", set()):
                continue
            if user_id and str(info.get("user_id")) != str(user_id):
                continue
            conn_time = info.get("connected_at", now_ts)
            clients.append({
                "client_id": info.get("client_id"),
                "user_id": info.get("user_id"),
                "session_id": info.get("session_id"),
                "channels": sorted(list(info.get("channels", set()))),
                "remote_ip": info.get("remote_ip"),
                "connected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(conn_time)),
                "uptime_seconds": round(now_ts - conn_time, 1)
            })
        return clients

    def get_channels_summary(self) -> list[dict]:
        """Returns list of active channels with their subscriber counts."""
        summary = []
        for channel, sockets in self.channel_sockets.items():
            summary.append({
                "name": channel,
                "subscriber_count": len(sockets)
            })
        summary.sort(key=lambda x: x["name"])
        return summary

    async def disconnect_client(self, client_id: str, reason: str = "Disconnected by admin") -> bool:
        """Forcefully disconnects a specific client by client_id."""
        target_ws = None
        for ws, info in list(self.active_connections.items()):
            if info.get("client_id") == client_id:
                target_ws = ws
                break
        if not target_ws:
            return False

        try:
            await target_ws.send(json.dumps({
                "event": "disconnected",
                "data": {"reason": reason}
            }))
        except Exception:
            pass
        self.unregister(target_ws)
        try:
            if hasattr(target_ws, "close"):
                await target_ws.close(1000)
        except Exception:
            pass
        return True

    async def disconnect_user(self, user_id: str, reason: str = "Disconnected by admin") -> int:
        """Forcefully disconnects all sockets belonging to a specific user_id."""
        user_str = str(user_id)
        sockets = list(self.user_sockets.get(user_str, set()))
        if not sockets:
            return 0
        count = 0
        for ws in sockets:
            try:
                await ws.send(json.dumps({
                    "event": "disconnected",
                    "data": {"reason": reason}
                }))
            except Exception:
                pass
            self.unregister(ws)
            try:
                if hasattr(ws, "close"):
                    await ws.close(1000)
            except Exception:
                pass
            count += 1
        return count

    def clear_grace_sessions(self) -> int:
        """Purges all sessions currently waiting in grace period."""
        count = 0
        for sid, data in list(self.sessions.items()):
            task = data.get("disconnect_task")
            if task and not task.done():
                task.cancel()
            del self.sessions[sid]
            count += 1
        return count


manager = ConnectionManager(grace_period_seconds=config.get("grace_period_seconds", 180))


async def handle_pubsub_message(envelope: dict) -> int:
    """
    Callback executed when an event envelope is received from the Pub/Sub bus.
    Dispatches to local WebSocket clients.
    """
    message = envelope.get("message", {})
    user_ids = envelope.get("user_ids")
    channels = envelope.get("channels")
    return await manager.local_broadcast(message, user_ids=user_ids, channels=channels)


# Wire up handler for local in-memory bus directly so test clients work out-of-the-box
if isinstance(pubsub, LocalPubSub):
    pubsub._on_message = handle_pubsub_message


async def reconfigure_pubsub(new_scale_mode: str, new_params: dict) -> tuple[bool, str]:
    """
    Dynamically switches or reconfigures PubSubBackend at runtime without dropping WebSocket connections.
    """
    global pubsub, scale_mode
    target_mode = "scalable" if new_scale_mode in ("scalable", "cluster") else "autonome"

    if target_mode == "scalable":
        redis_url = new_params.get("redis_url", config.get("redis_url"))
        redis_host = new_params.get("redis_host", config.get("redis_host", "localhost"))
        redis_port = int(new_params.get("redis_port", config.get("redis_port", 6379)))
        redis_password = new_params.get("redis_password", config.get("redis_password"))
        redis_db = int(new_params.get("redis_db", config.get("redis_db", 0)))
        redis_channel = new_params.get("redis_channel", config.get("redis_channel", "melodi:ws:events"))

        test_backend = RedisPubSub(
            redis_url=redis_url,
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=redis_db,
            channel=redis_channel
        )
        ok, err = await test_backend.check_connection()
        if not ok:
            return False, f"Impossible d'activer Redis scalable : {err}"

        # Cleanly stop previous backend
        try:
            await pubsub.stop()
        except Exception as e:
            logger.warning(f"Error stopping old pubsub backend: {e}")

        # Start new Redis backend
        try:
            await test_backend.start(handle_pubsub_message)
            pubsub = test_backend
            scale_mode = "scalable"
            config["scale_mode"] = "scalable"
            logger.info("[Melodi WebSocket] Basculement dynamique réussi vers Redis PubSub (scalable)")
            return True, "PubSub scalable Redis activé avec succès"
        except Exception as exc:
            return False, f"Échec de démarrage du listener Redis : {exc}"
    else:
        # Switch to autonome
        try:
            await pubsub.stop()
        except Exception as e:
            logger.warning(f"Error stopping old pubsub backend: {e}")
        local_backend = LocalPubSub()
        await local_backend.start(handle_pubsub_message)
        pubsub = local_backend
        scale_mode = "autonome"
        config["scale_mode"] = "autonome"
        logger.info("[Melodi WebSocket] Basculement dynamique réussi vers LocalPubSub (autonome)")
        return True, "PubSub autonome in-memory activé avec succès"


@app.before_serving
async def startup():
    """
    Startup checks and pubsub subscriber loop initialization.
    If scale_mode is 'scalable', Redis connectivity is strictly verified.
    If Redis is unreachable or fails authentication, the server MUST NOT start.
    """
    logger.info(f"[Melodi WebSocket] Démarrage en mode '{scale_mode}' (node_id={node_id})")

    if scale_mode == "scalable":
        logger.info("[Melodi WebSocket] Vérification obligatoire de la connexion Redis...")
        ok, err = await pubsub.check_connection()
        if not ok:
            logger.critical(
                f"[Melodi WebSocket] ÉCHEC CRITIQUE : Impossible de joindre Redis en mode scalable ({err}). "
                f"Le serveur ne démarrera pas."
            )
            sys.exit(1)
        logger.info("[Melodi WebSocket] Connexion Redis validée avec succès.")

    try:
        await pubsub.start(handle_pubsub_message)
        logger.info(f"[Melodi WebSocket] PubSub backend '{pubsub.name}' opérationnel.")
    except Exception as exc:
        logger.critical(f"[Melodi WebSocket] Échec d'activation du PubSub : {exc}")
        sys.exit(1)


@app.after_serving
async def shutdown():
    """Clean shutdown of PubSub listener and network pools."""
    logger.info("[Melodi WebSocket] Arrêt du backend PubSub...")
    await pubsub.stop()
    logger.info("[Melodi WebSocket] Backend PubSub arrêté.")


def authenticate_jwt(token: str) -> dict | None:
    """
    Validates JWT token using PyJWT or fallback base64 payload decode.
    """
    if not token:
        return None
    try:
        import jwt
        try:
            return jwt.decode(token, config.get("secret", ""), algorithms=["HS256"])
        except Exception:
            return jwt.decode(token, options={"verify_signature": False})
    except Exception:
        pass

    try:
        import base64
        parts = token.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1]
            padded = payload_b64 + "=" * (-len(payload_b64) % 4)
            return json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    except Exception as e:
        logger.warning(f"[Melodi WebSocket] Token decode error: {e}")
    return None


def check_internal_auth(headers) -> bool:
    """
    Authenticates internal API calls via X-Internal-Secret header or Bearer secret token.
    """
    expected_secret = config.get("secret")
    secret_header = headers.get("X-Internal-Secret")
    if secret_header and secret_header == expected_secret:
        return True

    auth_header = headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        return token == expected_secret

    return False


def is_allowed_internal_ip(ip_str: str | None) -> bool:
    """
    Checks if an IP belongs to loopback or private networks (e.g. Docker bridge, LAN).
    """
    if not ip_str or ip_str in ("127.0.0.1", "::1", "localhost", "testclient", "<local>"):
        return True
    try:
        ip = ipaddress.ip_address(ip_str)
        return ip.is_loopback or ip.is_private
    except ValueError:
        return False


@app.before_request
async def limit_internal_to_localhost():
    """
    Ensures internal administrative endpoints are accessible only from localhost,
    private internal networks (such as Docker bridge/containers), or with valid secret.
    """
    from quart import has_request_context
    if has_request_context() and request.path.startswith("/internal"):
        # If request has valid secret, allow it regardless of IP
        if check_internal_auth(request.headers):
            return None

        remote_ip = request.remote_addr
        allow_private = config.get("allow_private_internal", True)
        if allow_private and is_allowed_internal_ip(remote_ip):
            return None

        logger.warning(f"[Melodi WebSocket] Forbidden access attempt from IP: {remote_ip}")
        return jsonify({"status": "error", "message": "Forbidden: Private network or localhost access only"}), 403


# ==============================================================================
# Public & Health Endpoints
# ==============================================================================

@app.route("/", methods=["GET"])
@app.route("/health", methods=["GET"])
async def root_health():
    """
    Public health check endpoint for Docker container checks, load balancers, and monitoring.
    """
    redis_status = None
    if scale_mode == "scalable":
        ok, msg = await pubsub.check_connection()
        redis_status = "connected" if ok else f"error: {msg}"

    return jsonify({
        "status": "ok",
        "service": "melodi_websocket",
        "version": "1.0.0",
        "scale_mode": scale_mode,
        "node_id": node_id,
        "redis_status": redis_status,
        "active_connections": len(manager.active_connections),
        "active_channels": len(manager.channel_sockets)
    }), 200


@app.route("/internal/health", methods=["GET"])
async def api_health():
    """
    Health check endpoint for internal MELODI ERP status checks.
    """
    redis_status = None
    if scale_mode == "scalable":
        ok, msg = await pubsub.check_connection()
        redis_status = "connected" if ok else f"error: {msg}"

    return jsonify({
        "status": "ok",
        "service": "melodi_websocket",
        "scale_mode": scale_mode,
        "node_id": node_id,
        "redis_status": redis_status,
        "active_connections": len(manager.active_connections),
        "active_channels": len(manager.channel_sockets)
    }), 200


# ==============================================================================
# Core Broadcast & Presence Endpoints
# ==============================================================================

@app.route("/internal/broadcast", methods=["POST"])
@app.route("/internal/emit", methods=["POST"])
async def api_broadcast():
    """
    Internal broadcast route for the main backend.
    Dispatches to local clients in 'autonome' mode, or publishes to Redis pub/sub
    in 'scalable' mode to coordinate across multiple service nodes.
    Payload:
    {
        "event": "stock_updated",
        "data": {"item_id": 101, "stock": 42},
        "user_ids": [1, 2],       # Optional
        "channels": ["inventory"]  # Optional
    }
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    payload = await request.get_json()
    if not payload or "event" not in payload:
        return jsonify({"status": "error", "message": "Bad Request: Missing 'event' field"}), 400

    event_name = payload.get("event")
    event_data = payload.get("data", {})
    user_ids = payload.get("user_ids")
    channels = payload.get("channels")

    message = {
        "event": event_name,
        "data": event_data
    }

    envelope = {
        "origin_node": node_id,
        "message": message,
        "user_ids": user_ids,
        "channels": channels
    }

    count = await pubsub.publish(envelope)

    return jsonify({
        "status": "success",
        "recipients_count": count,
        "event": event_name,
        "scale_mode": scale_mode,
        "node_id": node_id
    }), 200


@app.route("/internal/presence", methods=["GET"])
async def api_presence():
    """
    Returns current online users and sessions currently in grace period.
    """
    active_uids = list(manager.user_sockets.keys())
    grace_sessions = [
        {"session_id": sid, "user_id": data.get("user_id")}
        for sid, data in manager.sessions.items()
        if data.get("disconnect_task") and not data["disconnect_task"].done()
    ]
    return jsonify({
        "status": "ok",
        "online_user_ids": active_uids,
        "grace_sessions": grace_sessions,
        "total_active_sockets": len(manager.active_connections),
        "total_sessions": len(manager.sessions)
    }), 200


# ==============================================================================
# Administration & Configuration Management Endpoints (for Melodi ERP)
# ==============================================================================

@app.route("/internal/config", methods=["GET"])
async def api_get_config():
    """
    Returns current running configuration and storage status for Melodi ERP.
    Secrets are masked unless ?include_secrets=true is passed with valid authorization.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    include_secrets = request.args.get("include_secrets", "false").lower() in ("true", "1", "yes")
    structured = config_mgr.get_structured(include_secrets=include_secrets)

    uptime_sec = round(time.time() - config_mgr.start_time, 1)

    return jsonify({
        "status": "ok",
        "node_id": node_id,
        "uptime_seconds": uptime_sec,
        "scale_mode": scale_mode,
        "active_connections": len(manager.active_connections),
        "active_channels": len(manager.channel_sockets),
        "config": structured
    }), 200


@app.route("/internal/config", methods=["POST", "PUT"])
async def api_update_config():
    """
    Updates configuration dynamically from Melodi ERP.
    Applies live settings immediately (grace_period, log_level, secret, pubsub scale mode)
    and persists them to storage/config.json.
    Payload:
    {
        "grace_period_seconds": 120,
        "log_level": "DEBUG",
        "scale_mode": "scalable",
        "redis_host": "127.0.0.1",
        "redis_port": 6379,
        "redis_password": "...",
        "persist": true
    }
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    payload = await request.get_json()
    if not payload or not isinstance(payload, dict):
        return jsonify({"status": "error", "message": "Invalid JSON payload"}), 400

    persist = payload.get("persist", True)
    ok, updated_keys, restart_required, msg = config_mgr.update_config(payload, persist=persist)

    if not ok:
        return jsonify({"status": "error", "message": msg}), 400

    # Apply live dynamic updates
    global config
    config = config_mgr.config
    config["node_id"] = node_id

    # 1. Update grace period
    if "grace_period_seconds" in updated_keys:
        manager.grace_period_seconds = config["grace_period_seconds"]
        logger.info(f"[Melodi WebSocket] Période de grâce mise à jour à {manager.grace_period_seconds}s")

    # 2. Update log level
    if "log_level" in updated_keys:
        new_lvl_str = config.get("log_level", "INFO").upper()
        new_lvl = getattr(logging, new_lvl_str, logging.INFO)
        logging.getLogger().setLevel(new_lvl)
        logger.setLevel(new_lvl)
        logger.info(f"[Melodi WebSocket] Niveau de journalisation mis à jour à {new_lvl_str}")

    # 3. Dynamic PubSub reconfigure if scale_mode or Redis settings changed
    pubsub_keys = {"scale_mode", "redis_url", "redis_host", "redis_port", "redis_password", "redis_db", "redis_channel"}
    if pubsub_keys.intersection(updated_keys):
        ps_ok, ps_msg = await reconfigure_pubsub(config.get("scale_mode", "autonome"), config)
        if not ps_ok:
            return jsonify({
                "status": "warning",
                "message": f"Paramètres enregistrés mais échec du basculement PubSub: {ps_msg}",
                "updated_keys": updated_keys,
                "restart_required": True,
                "config": config_mgr.get_structured(include_secrets=False)
            }), 200

    return jsonify({
        "status": "success",
        "message": msg,
        "updated_keys": updated_keys,
        "restart_required": restart_required,
        "config": config_mgr.get_structured(include_secrets=False)
    }), 200


@app.route("/internal/config/test", methods=["POST"])
async def api_test_config():
    """
    Tests proposed configuration (e.g. Redis connectivity or Melodi ERP connection)
    before applying changes.
    Payload:
    {
        "target": "redis",
        "redis_host": "localhost",
        "redis_port": 6379,
        "redis_password": "...",
        "redis_db": 0
    }
    or
    {
        "target": "melodi",
        "melodi_host": "localhost",
        "melodi_port": 3000
    }
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    payload = await request.get_json() or {}
    target = payload.get("target", "redis").lower()

    if target == "redis":
        r_host = payload.get("redis_host") or config.get("redis_host", "localhost")
        r_port = int(payload.get("redis_port") or config.get("redis_port", 6379))
        r_pass = payload.get("redis_password") or config.get("redis_password")
        r_db = int(payload.get("redis_db") or config.get("redis_db", 0))
        r_url = payload.get("redis_url") or config.get("redis_url")

        success, latency_ms, message = await ConfigManager.test_redis_connection(
            host=r_host,
            port=r_port,
            password=r_pass,
            db=r_db,
            redis_url=r_url
        )
        return jsonify({
            "status": "ok" if success else "error",
            "success": success,
            "target": "redis",
            "latency_ms": latency_ms,
            "message": message
        }), 200

    elif target in ("melodi", "erp"):
        m_host = payload.get("melodi_host") or config.get("melodi_host", "localhost")
        m_port = int(payload.get("melodi_port") or config.get("melodi_port", 3000))

        success, latency_ms, message = await ConfigManager.test_melodi_connection(
            host=m_host,
            port=m_port
        )
        return jsonify({
            "status": "ok" if success else "error",
            "success": success,
            "target": "melodi",
            "latency_ms": latency_ms,
            "message": message
        }), 200

    return jsonify({"status": "error", "message": f"Unknown target: '{target}'"}), 400


@app.route("/internal/config/reset", methods=["POST"])
async def api_reset_config():
    """
    Resets running and persisted configuration back to startup defaults.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    config_mgr.reset_to_startup()
    global config
    config = config_mgr.config
    config["node_id"] = node_id

    manager.grace_period_seconds = config.get("grace_period_seconds", 180)
    await reconfigure_pubsub(config.get("scale_mode", "autonome"), config)

    return jsonify({
        "status": "success",
        "message": "Configuration réinitialisée aux valeurs de démarrage",
        "config": config_mgr.get_structured(include_secrets=False)
    }), 200


@app.route("/internal/stats", methods=["GET"])
@app.route("/internal/admin/stats", methods=["GET"])
async def api_admin_stats():
    """
    Detailed operational metrics for Melodi ERP administration dashboard.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    now_ts = time.time()
    uptime_sec = round(now_ts - config_mgr.start_time, 1)

    redis_status = None
    if scale_mode == "scalable":
        ok, msg = await pubsub.check_connection()
        redis_status = "connected" if ok else f"error: {msg}"

    # Active sessions in grace period
    grace_count = sum(
        1 for d in manager.sessions.values()
        if d.get("disconnect_task") and not d["disconnect_task"].done()
    )

    # Memory usage if resource module is available
    memory_mb = None
    try:
        import resource
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        # On Linux ru_maxrss is in kilobytes
        memory_mb = round(rusage.ru_maxrss / 1024.0, 2)
    except Exception:
        pass

    return jsonify({
        "status": "ok",
        "service": "melodi_websocket",
        "node_id": node_id,
        "pid": os.getpid(),
        "uptime_seconds": uptime_sec,
        "scale_mode": scale_mode,
        "redis_status": redis_status,
        "connections": {
            "active": len(manager.active_connections),
            "total_accepted": manager.total_connections_accepted,
            "max_connections": config.get("max_connections", 10000)
        },
        "messages": {
            "total_received": manager.total_messages_received,
            "total_sent": manager.total_messages_sent
        },
        "channels": {
            "total_active": len(manager.channel_sockets),
            "list": manager.get_channels_summary()
        },
        "users": {
            "active_users_count": len(manager.user_sockets),
            "online_user_ids": list(manager.user_sockets.keys())
        },
        "sessions": {
            "total_sessions": len(manager.sessions),
            "grace_sessions_count": grace_count,
            "grace_period_seconds": manager.grace_period_seconds
        },
        "system": {
            "memory_rss_mb": memory_mb,
            "log_level": config.get("log_level", "INFO"),
            "config_file": config_mgr.storage_file_path
        }
    }), 200


@app.route("/internal/admin/channels", methods=["GET"])
async def api_admin_channels():
    """
    Lists all active broadcast channels and subscriber counts.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    summary = manager.get_channels_summary()
    return jsonify({
        "status": "ok",
        "total_channels": len(summary),
        "channels": summary
    }), 200


@app.route("/internal/admin/clients", methods=["GET"])
async def api_admin_clients():
    """
    Lists connected WebSocket clients, optionally filtered by ?channel= or ?user_id=.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    channel = request.args.get("channel")
    user_id = request.args.get("user_id")

    clients = manager.get_clients(channel=channel, user_id=user_id)
    return jsonify({
        "status": "ok",
        "total_clients": len(clients),
        "clients": clients
    }), 200


@app.route("/internal/admin/clients/disconnect", methods=["POST"])
async def api_admin_disconnect_client():
    """
    Forcefully disconnects a specific client or user from Melodi ERP.
    Payload:
    { "client_id": "client_12345", "reason": "Session expired" }
    or
    { "user_id": "42", "reason": "Account suspended" }
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    payload = await request.get_json() or {}
    reason = payload.get("reason", "Disconnected by administrator")
    client_id = payload.get("client_id")
    user_id = payload.get("user_id")

    if client_id:
        success = await manager.disconnect_client(client_id, reason=reason)
        if success:
            return jsonify({"status": "success", "message": f"Client '{client_id}' disconnected"}), 200
        return jsonify({"status": "error", "message": f"Client '{client_id}' not found"}), 404

    elif user_id:
        count = await manager.disconnect_user(user_id, reason=reason)
        return jsonify({
            "status": "success",
            "message": f"Disconnected {count} connection(s) for user '{user_id}'",
            "disconnected_count": count
        }), 200

    return jsonify({"status": "error", "message": "Missing 'client_id' or 'user_id' in payload"}), 400


@app.route("/internal/admin/sessions/clear-grace", methods=["POST"])
async def api_admin_clear_grace():
    """
    Manually purges all reconnecting sessions currently within the grace period.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    count = manager.clear_grace_sessions()
    return jsonify({
        "status": "success",
        "message": f"{count} grace session(s) cleared",
        "cleared_count": count
    }), 200


@app.route("/internal/admin/reload", methods=["POST"])
async def api_admin_reload():
    """
    Reloads persistent configuration from disk (storage/config.json) without restarting.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    file_config = config_mgr.load_from_file()
    if not file_config:
        return jsonify({"status": "warning", "message": "No persistent config file found or empty"}), 200

    ok, updated_keys, restart_required, msg = config_mgr.update_config(file_config, persist=False)
    if "grace_period_seconds" in updated_keys:
        manager.grace_period_seconds = config_mgr.config.get("grace_period_seconds", 180)

    return jsonify({
        "status": "success",
        "message": "Configuration reloaded from disk",
        "updated_keys": updated_keys,
        "restart_required": restart_required,
        "config": config_mgr.get_structured(include_secrets=False)
    }), 200


@app.route("/internal/admin/restart", methods=["POST"])
async def api_admin_restart():
    """
    Triggers a graceful shutdown and restart sequence for container/supervisor orchestrators.
    """
    if not check_internal_auth(request.headers):
        return jsonify({"status": "error", "message": "Unauthorized: Invalid secret"}), 401

    async def _delayed_restart():
        await asyncio.sleep(0.5)
        logger.info("[Melodi WebSocket] Arrêt programmé demandé par Melodi ERP.")
        # Trigger clean exit
        os._exit(0)

    asyncio.create_task(_delayed_restart())

    return jsonify({
        "status": "success",
        "message": "Redémarrage du service WebSocket initié"
    }), 200


# ==============================================================================
# WebSocket Client Connection Handler
# ==============================================================================

@app.websocket("/ws")
async def ws_endpoint():
    """
    WebSocket connection handler.
    Clients can pass ?token=...&session_id=... in the query string or send an 'auth' JSON message.
    """
    token = websocket.args.get("token")
    session_id = websocket.args.get("session_id")
    user_id = None
    remote_ip = websocket.remote_addr

    if token:
        payload = authenticate_jwt(token)
        if payload:
            user_id = payload.get("user_id") or payload.get("id") or payload.get("sub")

    ws = websocket._get_current_object()
    client_id = manager.register(ws, user_id=user_id, session_id=session_id, remote_ip=remote_ip)

    # Connection established response
    await ws.send(json.dumps({
        "event": "connected",
        "data": {
            "client_id": client_id,
            "session_id": session_id,
            "authenticated": user_id is not None,
            "user_id": user_id,
            "grace_period_seconds": manager.grace_period_seconds
        }
    }))
    manager.total_messages_sent += 1

    try:
        while True:
            raw_msg = await ws.receive()
            if not raw_msg:
                break
            manager.total_messages_received += 1

            try:
                msg = json.loads(raw_msg)
                action = msg.get("action") or msg.get("type")

                if action == "ping":
                    await ws.send(json.dumps({"event": "pong", "data": {}}))
                    manager.total_messages_sent += 1

                elif action == "auth":
                    msg_token = msg.get("token")
                    msg_sid = msg.get("session_id") or session_id
                    payload = authenticate_jwt(msg_token) if msg_token else None
                    if payload:
                        new_user_id = payload.get("user_id") or payload.get("id") or payload.get("sub")
                        manager.unregister(ws)
                        manager.register(ws, user_id=new_user_id, session_id=msg_sid, remote_ip=remote_ip)
                        await ws.send(json.dumps({
                            "event": "authenticated",
                            "data": {"user_id": new_user_id, "session_id": msg_sid}
                        }))
                        manager.total_messages_sent += 1
                    else:
                        await ws.send(json.dumps({
                            "event": "error",
                            "data": {"message": "Authentication failed: invalid token"}
                        }))
                        manager.total_messages_sent += 1

                elif action == "subscribe":
                    channel = msg.get("channel")
                    if channel:
                        manager.subscribe(ws, channel)
                        await ws.send(json.dumps({
                            "event": "subscribed",
                            "data": {"channel": channel}
                        }))
                        manager.total_messages_sent += 1

                elif action == "unsubscribe":
                    channel = msg.get("channel")
                    if channel:
                        manager.unsubscribe(ws, channel)
                        await ws.send(json.dumps({
                            "event": "unsubscribed",
                            "data": {"channel": channel}
                        }))
                        manager.total_messages_sent += 1

                else:
                    await ws.send(json.dumps({
                        "event": "info",
                        "data": {"message": "For state modifications, please use the main REST API."}
                    }))
                    manager.total_messages_sent += 1

            except json.JSONDecodeError:
                await ws.send(json.dumps({"event": "error", "data": {"message": "Invalid JSON format"}}))
                manager.total_messages_sent += 1

    except Exception as exc:
        logger.info(f"[Melodi WebSocket] Connection closed ({client_id}): {exc}")
    finally:
        manager.unregister(ws)


if __name__ == "__main__":
    logger.info(f"Starting Melodi WebSocket Service on http://{config['host']}:{config['port']}")
    app.run(host=config["host"], port=config["port"])


import os
import sys
import json
import uuid
import asyncio
import logging
import argparse
import ipaddress
from typing import Dict, Set, Any
from quart import Quart, request, jsonify, websocket
from pubsub import PubSubBackend, LocalPubSub, RedisPubSub

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

default_port = int(os.environ.get("PORT") or os.environ.get("MELODI_WEBSOCKET_PORT") or 8002)
default_secret = os.environ.get("SECRET") or os.environ.get("MELODI_WEBSOCKET_SECRET") or "melodi_ws_secret_456"
default_scale_mode = os.environ.get("WEBSOCKET_SCALE_MODE", "autonome").strip().lower()

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Melodi WebSocket Real-Time Service")
parser.add_argument("--secret", type=str, default=default_secret, help="Internal shared secret key")
parser.add_argument("--port", type=int, default=default_port, help="WebSocket service port")
parser.add_argument("--melodi-port", type=int, default=3000, help="Main Melodi application port")
parser.add_argument("--melodi-host", type=str, default="localhost", help="Main Melodi application host")
parser.add_argument("--melodi-platform", type=str, default="linux", help="Main Melodi application platform")
parser.add_argument("--env", type=str, default="dev", help="Environment")
parser.add_argument("--scale-mode", type=str, default=default_scale_mode, choices=["autonome", "scalable", "standalone", "cluster"], help="Scale mode: autonome or scalable")
parser.add_argument("--redis-url", type=str, default=os.environ.get("REDIS_URL"), help="Redis URL for scalable mode (e.g. redis://:password@host:6379/0)")
parser.add_argument("--redis-host", type=str, default=os.environ.get("REDIS_HOST", "localhost"), help="Redis host")
parser.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", "6379")), help="Redis port")
parser.add_argument("--redis-password", type=str, default=os.environ.get("REDIS_PASSWORD"), help="Redis password")
parser.add_argument("--redis-db", type=int, default=int(os.environ.get("REDIS_DB", "0")), help="Redis database index")
parser.add_argument("--redis-channel", type=str, default=os.environ.get("REDIS_WS_CHANNEL", "melodi:ws:events"), help="Redis pub/sub channel name")
args, unknown = parser.parse_known_args()

scale_mode = "scalable" if args.scale_mode in ("scalable", "cluster") else "autonome"

config = {
    "secret": args.secret,
    "port": args.port,
    "melodi_port": args.melodi_port,
    "env": args.env,
    "host": "0.0.0.0",
    "scale_mode": scale_mode,
    "node_id": node_id
}

# Initialize Pub/Sub Backend
if scale_mode == "scalable":
    pubsub: PubSubBackend = RedisPubSub(
        redis_url=args.redis_url,
        host=args.redis_host,
        port=args.redis_port,
        password=args.redis_password,
        db=args.redis_db,
        channel=args.redis_channel
    )
else:
    pubsub: PubSubBackend = LocalPubSub()


class ConnectionManager:
    """
    Registry and fanout manager for real-time WebSocket connections.
    Tracks active connections, user mappings, and channel subscriptions.
    """
    def __init__(self):
        # socket object -> client metadata dictionary
        self.active_connections: Dict[Any, Dict[str, Any]] = {}
        # user_id -> Set of socket objects
        self.user_sockets: Dict[str, Set[Any]] = {}
        # channel_name -> Set of socket objects
        self.channel_sockets: Dict[str, Set[Any]] = {}

    def register(self, ws, user_id: str | None = None) -> str:
        client_id = f"client_{id(ws)}"
        self.active_connections[ws] = {
            "client_id": client_id,
            "user_id": str(user_id) if user_id else None,
            "channels": set()
        }
        if user_id:
            user_str = str(user_id)
            if user_str not in self.user_sockets:
                self.user_sockets[user_str] = set()
            self.user_sockets[user_str].add(ws)

        logger.info(f"[Melodi WebSocket] Client registered: {client_id} (user_id: {user_id})")
        return client_id

    def subscribe(self, ws, channel: str):
        if ws in self.active_connections:
            self.active_connections[ws]["channels"].add(channel)
            if channel not in self.channel_sockets:
                self.channel_sockets[channel] = set()
            self.channel_sockets[channel].add(ws)
            logger.info(f"[Melodi WebSocket] Client {self.active_connections[ws]['client_id']} subscribed to '{channel}'")

    def unsubscribe(self, ws, channel: str):
        if ws in self.active_connections:
            self.active_connections[ws]["channels"].discard(channel)
            if channel in self.channel_sockets:
                self.channel_sockets[channel].discard(ws)

    def unregister(self, ws):
        if ws in self.active_connections:
            info = self.active_connections.pop(ws)
            client_id = info.get("client_id")
            user_id = info.get("user_id")

            if user_id and user_id in self.user_sockets:
                self.user_sockets[user_id].discard(ws)
                if not self.user_sockets[user_id]:
                    del self.user_sockets[user_id]

            for channel in info.get("channels", set()):
                if channel in self.channel_sockets:
                    self.channel_sockets[channel].discard(ws)
                    if not self.channel_sockets[channel]:
                        del self.channel_sockets[channel]

            logger.info(f"[Melodi WebSocket] Client unregistered: {client_id}")

    async def _safe_send(self, ws, message: dict):
        try:
            await ws.send(json.dumps(message))
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


manager = ConnectionManager()


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
    secret_header = headers.get("X-Internal-Secret")
    if secret_header and secret_header == config["secret"]:
        return True

    auth_header = headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        token = auth_header.split(" ")[1]
        return token == config["secret"]

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
        allow_private = os.environ.get("ALLOW_PRIVATE_INTERNAL", "true").lower() in ("true", "1", "yes")
        if allow_private and is_allowed_internal_ip(remote_ip):
            return None

        logger.warning(f"[Melodi WebSocket] Forbidden access attempt from IP: {remote_ip}")
        return jsonify({"status": "error", "message": "Forbidden: Private network or localhost access only"}), 403


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


@app.websocket("/ws")
async def ws_endpoint():
    """
    WebSocket connection handler.
    Clients can pass ?token=... in the query string or send an 'auth' JSON message.
    """
    token = websocket.args.get("token")
    user_id = None

    if token:
        payload = authenticate_jwt(token)
        if payload:
            user_id = payload.get("user_id") or payload.get("id") or payload.get("sub")

    ws = websocket._get_current_object()
    client_id = manager.register(ws, user_id=user_id)

    # Connection established response
    await ws.send(json.dumps({
        "event": "connected",
        "data": {
            "client_id": client_id,
            "authenticated": user_id is not None,
            "user_id": user_id
        }
    }))

    try:
        while True:
            raw_msg = await ws.receive()
            if not raw_msg:
                break

            try:
                msg = json.loads(raw_msg)
                action = msg.get("action") or msg.get("type")

                if action == "ping":
                    await ws.send(json.dumps({"event": "pong", "data": {}}))

                elif action == "auth":
                    msg_token = msg.get("token")
                    payload = authenticate_jwt(msg_token) if msg_token else None
                    if payload:
                        new_user_id = payload.get("user_id") or payload.get("id") or payload.get("sub")
                        manager.unregister(ws)
                        manager.register(ws, user_id=new_user_id)
                        await ws.send(json.dumps({
                            "event": "authenticated",
                            "data": {"user_id": new_user_id}
                        }))
                    else:
                        await ws.send(json.dumps({
                            "event": "error",
                            "data": {"message": "Authentication failed: invalid token"}
                        }))

                elif action == "subscribe":
                    channel = msg.get("channel")
                    if channel:
                        manager.subscribe(ws, channel)
                        await ws.send(json.dumps({
                            "event": "subscribed",
                            "data": {"channel": channel}
                        }))

                elif action == "unsubscribe":
                    channel = msg.get("channel")
                    if channel:
                        manager.unsubscribe(ws, channel)
                        await ws.send(json.dumps({
                            "event": "unsubscribed",
                            "data": {"channel": channel}
                        }))

                else:
                    await ws.send(json.dumps({
                        "event": "info",
                        "data": {"message": "For state modifications, please use the main REST API."}
                    }))

            except json.JSONDecodeError:
                await ws.send(json.dumps({"event": "error", "data": {"message": "Invalid JSON format"}}))

    except Exception as exc:
        logger.info(f"[Melodi WebSocket] Connection closed ({client_id}): {exc}")
    finally:
        manager.unregister(ws)


if __name__ == "__main__":
    logger.info(f"Starting Melodi WebSocket Service on http://{config['host']}:{config['port']}")
    app.run(host=config["host"], port=config["port"])

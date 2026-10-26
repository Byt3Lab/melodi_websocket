
import os
import sys
import json
import asyncio
import logging
import argparse
from typing import Dict, Set, Any
from quart import Quart, request, jsonify, websocket

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("melodi_websocket")

app = Quart(__name__)

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Melodi WebSocket Real-Time Service")
parser.add_argument("--secret", type=str, default="melodi_ws_secret_456", help="Internal shared secret key")
parser.add_argument("--port", type=int, default=8002, help="WebSocket service port")
parser.add_argument("--melodi-port", type=int, default=3000, help="Main Melodi application port")
parser.add_argument("--melodi-host", type=str, default="localhost", help="Main Melodi application host")
parser.add_argument("--melodi-platform", type=str, default="linux", help="Main Melodi application platform")
parser.add_argument("--env", type=str, default="dev", help="Environment")
args, unknown = parser.parse_known_args()

config = {
    "secret": args.secret,
    "port": args.port,
    "melodi_port": args.melodi_port,
    "env": args.env,
    "host": "0.0.0.0"
}


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

    async def broadcast(self, message: dict, user_ids: list | None = None, channels: list | None = None) -> int:
        """
        Broadcasts message to specific user_ids, subscribed channels, or all clients if neither is specified.
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


manager = ConnectionManager()


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


@app.before_request
async def limit_internal_to_localhost():
    """
    Ensures internal administrative endpoints are accessible only from localhost.
    """
    from quart import has_request_context
    if has_request_context() and request.path.startswith("/internal"):
        remote_ip = request.remote_addr
        if remote_ip not in ("127.0.0.1", "::1", "localhost"):
            logger.warning(f"[Melodi WebSocket] Forbidden access attempt from IP: {remote_ip}")
            return jsonify({"status": "error", "message": "Forbidden: Localhost access only"}), 403


@app.route("/internal/health", methods=["GET"])
async def api_health():
    """
    Health check endpoint.
    """
    return jsonify({
        "status": "ok",
        "service": "melodi_websocket",
        "active_connections": len(manager.active_connections),
        "active_channels": len(manager.channel_sockets)
    }), 200


@app.route("/internal/broadcast", methods=["POST"])
@app.route("/internal/emit", methods=["POST"])
async def api_broadcast():
    """
    Internal broadcast route for the main backend.
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

    count = await manager.broadcast(message, user_ids=user_ids, channels=channels)

    return jsonify({
        "status": "success",
        "recipients_count": count,
        "event": event_name
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

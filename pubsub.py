"""
Pub/Sub Backends for Melodi WebSocket.
Supports:
- LocalPubSub (mode 'autonome'): In-memory fanout within a single process.
- RedisPubSub (mode 'scalable'): Distributed message bus via Redis Pub/Sub for horizontal scaling.
"""

from __future__ import annotations

import json
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Callable, Awaitable, Any

logger = logging.getLogger("melodi_websocket.pubsub")


class PubSubBackend(ABC):
    """Abstract interface for WebSocket Pub/Sub coordination."""

    @abstractmethod
    async def check_connection(self) -> tuple[bool, str]:
        """Validates broker connectivity before startup."""
        pass

    @abstractmethod
    async def start(self, on_message: Callable[[dict], Awaitable[None]]) -> None:
        """Starts the subscription listener task."""
        pass

    @abstractmethod
    async def publish(self, envelope: dict) -> int:
        """Publishes an event envelope to all instances."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stops listener and cleans up resources."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass


class LocalPubSub(PubSubBackend):
    """
    Default in-memory Pub/Sub backend for standalone / autonomous mode.
    Directly dispatches envelopes within the local process.
    """

    def __init__(self):
        self._on_message: Callable[[dict], Awaitable[None]] | None = None

    @property
    def name(self) -> str:
        return "autonome"

    async def check_connection(self) -> tuple[bool, str]:
        return True, "Mode autonome (in-memory) opérationnel"

    async def start(self, on_message: Callable[[dict], Awaitable[None]]) -> None:
        self._on_message = on_message
        logger.info("[PubSub] Initialisé en mode AUTONOME (in-memory).")

    async def publish(self, envelope: dict) -> int:
        if self._on_message:
            try:
                res = self._on_message(envelope)
                if asyncio.iscoroutine(res):
                    val = await res
                    return val if isinstance(val, int) else 1
                return res if isinstance(res, int) else 1
            except Exception as exc:
                logger.error(f"[PubSub] Erreur lors de la distribution locale : {exc}")
        return 0

    async def stop(self) -> None:
        self._on_message = None


class RedisPubSub(PubSubBackend):
    """
    Distributed Redis Pub/Sub backend for horizontal scaling (mode 'scalable').
    Coordinates event broadcast across N instances.
    """

    def __init__(
        self,
        redis_url: str | None = None,
        host: str = "localhost",
        port: int = 6379,
        password: str | None = None,
        db: int = 0,
        channel: str = "melodi:ws:events"
    ):
        self.redis_url = redis_url
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.channel = channel

        self._redis_client: Any = None
        self._pubsub: Any = None
        self._listen_task: asyncio.Task | None = None
        self._on_message: Callable[[dict], Awaitable[None]] | None = None
        self._running = False

    @property
    def name(self) -> str:
        return "scalable"

    def _get_client(self):
        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise RuntimeError(
                "Le paquet 'redis' est requis pour le mode scalable. "
                "Installez-le avec : pip install redis>=5.0.0"
            )

        if self.redis_url:
            return aioredis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_timeout=5.0
            )
        else:
            return aioredis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                decode_responses=True,
                socket_timeout=5.0
            )

    async def check_connection(self) -> tuple[bool, str]:
        """
        Tests connection to Redis.
        Returns (True, "OK") or (False, error_details).
        """
        client = None
        try:
            client = self._get_client()
            res = await client.ping()
            if res is True or str(res).upper() == "PONG":
                return True, "Connexion Redis validée avec succès"
            return False, f"Réponse Redis inattendue : {res}"
        except Exception as exc:
            return False, str(exc)
        finally:
            if client:
                try:
                    await client.aclose()
                except Exception:
                    pass

    async def start(self, on_message: Callable[[dict], Awaitable[None]]) -> None:
        """
        Validates Redis connection and starts the async listener loop.
        Raises ConnectionError if Redis is unavailable.
        """
        self._on_message = on_message

        # Strict startup check
        ok, err = await self.check_connection()
        if not ok:
            raise ConnectionError(
                f"[RedisPubSub] Échec critique de connexion à Redis en mode scalable : {err}"
            )

        self._redis_client = self._get_client()
        self._pubsub = self._redis_client.pubsub()
        await self._pubsub.subscribe(self.channel)

        self._running = True
        self._listen_task = asyncio.create_task(self._listen_loop())
        logger.info(f"[PubSub] Initialisé en mode SCALABLE sur canal Redis '{self.channel}'.")

    async def _listen_loop(self) -> None:
        """Background loop receiving distributed messages from Redis channel."""
        logger.info(f"[PubSub] Boucle d'écoute démarrée sur le canal '{self.channel}'")
        while self._running:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )
                if message and message.get("type") == "message":
                    raw_data = message.get("data")
                    if raw_data:
                        try:
                            envelope = json.loads(raw_data)
                            if self._on_message:
                                res = self._on_message(envelope)
                                if asyncio.iscoroutine(res):
                                    await res
                        except json.JSONDecodeError as jde:
                            logger.warning(f"[PubSub] Format JSON invalide reçu sur Redis : {jde}")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._running:
                    logger.warning(f"[PubSub] Erreur dans la boucle Redis : {exc}. Nouvelle tentative dans 1s...")
                    await asyncio.sleep(1.0)

    async def publish(self, envelope: dict) -> int:
        """Publishes envelope to Redis channel for multi-instance distribution."""
        if not self._redis_client:
            return 0
        try:
            payload = json.dumps(envelope)
            count = await self._redis_client.publish(self.channel, payload)
            return count
        except Exception as exc:
            logger.error(f"[PubSub] Erreur lors de la publication sur Redis : {exc}")
            return 0

    async def stop(self) -> None:
        """Shuts down Redis listener and releases connections."""
        self._running = False
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        if self._pubsub:
            try:
                await self._pubsub.unsubscribe(self.channel)
                await self._pubsub.aclose()
            except Exception:
                pass
            self._pubsub = None

        if self._redis_client:
            try:
                await self._redis_client.aclose()
            except Exception:
                pass
            self._redis_client = None
        logger.info("[PubSub] Connexions Redis fermées proprement.")

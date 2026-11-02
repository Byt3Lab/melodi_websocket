"""
Configuration Manager for Melodi WebSocket.
Handles configuration defaults, file-based persistence (storage/config.json),
environment variables, CLI overrides, dynamic updates, and connection testing.
"""

from __future__ import annotations

import os
import json
import time
import copy
import asyncio
import logging
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger("melodi_websocket.config")

# Default values
DEFAULT_CONFIG: Dict[str, Any] = {
    # Server / Network
    "host": "0.0.0.0",
    "port": 8002,
    "env": "dev",
    "log_level": "INFO",
    # Security
    "secret": "melodi_ws_secret_456",
    "allow_private_internal": True,
    # Melodi ERP Integration
    "melodi_host": "localhost",
    "melodi_port": 3000,
    "melodi_platform": "linux",
    # Scalability & PubSub
    "scale_mode": "autonome",  # "autonome" or "scalable"
    "redis_url": None,
    "redis_host": "localhost",
    "redis_port": 6379,
    "redis_password": None,
    "redis_db": 0,
    "redis_channel": "melodi:ws:events",
    # Runtime & Sessions
    "grace_period_seconds": 180,
    "max_connections": 10000,
    "ping_interval_seconds": 30,
}


def mask_secret(value: str | None) -> str:
    """Masks sensitive credentials for secure reporting."""
    if not value:
        return ""
    if len(value) <= 6:
        return "******"
    return f"{value[:3]}...{value[-3:]}"


class ConfigManager:
    """
    Centralized configuration manager supporting disk persistence,
    safe dynamic hot-reloading, secret masking, and diagnostics.
    """

    def __init__(self, config_dir: str = "storage", config_filename: str = "config.json"):
        self.config_dir = config_dir
        self.config_file = os.path.join(config_dir, config_filename)
        self.storage_file_path = os.path.abspath(self.config_file)
        self._config: Dict[str, Any] = copy.deepcopy(DEFAULT_CONFIG)
        self._initial_startup_config: Dict[str, Any] = {}
        self.start_time = time.time()

    def ensure_storage_dir(self) -> None:
        """Ensures storage directory exists."""
        try:
            os.makedirs(self.config_dir, exist_ok=True)
        except Exception as exc:
            logger.warning(f"[ConfigManager] Could not create storage directory '{self.config_dir}': {exc}")

    def load_from_file(self) -> Dict[str, Any]:
        """Loads persistent configuration from disk if present."""
        if os.path.isfile(self.config_file):
            try:
                with open(self.config_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        logger.info(f"[ConfigManager] Loaded persistent configuration from {self.config_file}")
                        return data
            except Exception as exc:
                logger.error(f"[ConfigManager] Error reading config file {self.config_file}: {exc}")
        return {}

    def save_to_file(self) -> bool:
        """Atomically saves current configuration to storage/config.json."""
        self.ensure_storage_dir()
        temp_file = f"{self.config_file}.tmp.{os.getpid()}"
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(self._config, f, indent=2)
            os.replace(temp_file, self.config_file)
            logger.info(f"[ConfigManager] Configuration saved successfully to {self.config_file}")
            return True
        except Exception as exc:
            logger.error(f"[ConfigManager] Failed to persist config to {self.config_file}: {exc}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
            return False

    def init_config(self, cli_args: Any = None) -> Dict[str, Any]:
        """
        Initializes configuration following precedence:
        Defaults < config.json (persisted) < Environment Variables < CLI Arguments
        """
        config = copy.deepcopy(DEFAULT_CONFIG)

        # 1. Merge persistent config file
        file_config = self.load_from_file()
        for k, v in file_config.items():
            if k in config and v is not None:
                config[k] = v

        # 2. Merge Environment variables
        env_mappings = {
            "PORT": ("port", int),
            "MELODI_WEBSOCKET_PORT": ("port", int),
            "SECRET": ("secret", str),
            "MELODI_WEBSOCKET_SECRET": ("secret", str),
            "WEBSOCKET_SCALE_MODE": ("scale_mode", str),
            "REDIS_URL": ("redis_url", str),
            "REDIS_HOST": ("redis_host", str),
            "REDIS_PORT": ("redis_port", int),
            "REDIS_PASSWORD": ("redis_password", str),
            "REDIS_DB": ("redis_db", int),
            "REDIS_WS_CHANNEL": ("redis_channel", str),
            "MELODI_PORT": ("melodi_port", int),
            "MELODI_HOST": ("melodi_host", str),
            "MELODI_PLATFORM": ("melodi_platform", str),
            "LOG_LEVEL": ("log_level", str),
            "ALLOW_PRIVATE_INTERNAL": ("allow_private_internal", lambda x: str(x).lower() in ("true", "1", "yes")),
            "GRACE_PERIOD_SECONDS": ("grace_period_seconds", int),
        }
        for env_var, (cfg_key, cast_fn) in env_mappings.items():
            val = os.environ.get(env_var)
            if val is not None and val != "":
                try:
                    config[cfg_key] = cast_fn(val)
                except (ValueError, TypeError):
                    pass

        # 3. Merge CLI arguments if provided
        if cli_args:
            if getattr(cli_args, "port", None) is not None:
                config["port"] = int(cli_args.port)
            if getattr(cli_args, "secret", None) is not None:
                config["secret"] = str(cli_args.secret)
            if getattr(cli_args, "melodi_port", None) is not None:
                config["melodi_port"] = int(cli_args.melodi_port)
            if getattr(cli_args, "melodi_host", None) is not None:
                config["melodi_host"] = str(cli_args.melodi_host)
            if getattr(cli_args, "melodi_platform", None) is not None:
                config["melodi_platform"] = str(cli_args.melodi_platform)
            if getattr(cli_args, "env", None) is not None:
                config["env"] = str(cli_args.env)
            if getattr(cli_args, "scale_mode", None) is not None:
                sm = str(cli_args.scale_mode).strip().lower()
                config["scale_mode"] = "scalable" if sm in ("scalable", "cluster") else "autonome"
            if getattr(cli_args, "redis_url", None) is not None:
                config["redis_url"] = cli_args.redis_url
            if getattr(cli_args, "redis_host", None) is not None:
                config["redis_host"] = str(cli_args.redis_host)
            if getattr(cli_args, "redis_port", None) is not None:
                config["redis_port"] = int(cli_args.redis_port)
            if getattr(cli_args, "redis_password", None) is not None:
                config["redis_password"] = str(cli_args.redis_password)
            if getattr(cli_args, "redis_db", None) is not None:
                config["redis_db"] = int(cli_args.redis_db)
            if getattr(cli_args, "redis_channel", None) is not None:
                config["redis_channel"] = str(cli_args.redis_channel)

        # Normalize scale_mode
        sm = str(config.get("scale_mode", "autonome")).strip().lower()
        config["scale_mode"] = "scalable" if sm in ("scalable", "cluster") else "autonome"

        self._file_existed_at_startup = os.path.isfile(self.config_file)
        self._config = config
        self._initial_startup_config = copy.deepcopy(config)
        return self._config

    @property
    def config(self) -> Dict[str, Any]:
        """Direct read access to current configuration dictionary."""
        return self._config

    def get_structured(self, include_secrets: bool = False) -> Dict[str, Any]:
        """
        Returns structured configuration suitable for Melodi ERP admin UI.
        Sensitive values are masked unless include_secrets is True.
        """
        c = self._config
        secret_val = c.get("secret") or ""
        redis_pass = c.get("redis_password") or ""

        return {
            "server": {
                "host": c.get("host", "0.0.0.0"),
                "port": c.get("port", 8002),
                "env": c.get("env", "dev"),
                "log_level": c.get("log_level", "INFO"),
            },
            "security": {
                "secret": secret_val if include_secrets else mask_secret(secret_val),
                "secret_configured": bool(secret_val),
                "allow_private_internal": bool(c.get("allow_private_internal", True)),
            },
            "integration": {
                "melodi_host": c.get("melodi_host", "localhost"),
                "melodi_port": c.get("melodi_port", 3000),
                "melodi_platform": c.get("melodi_platform", "linux"),
            },
            "pubsub": {
                "scale_mode": c.get("scale_mode", "autonome"),
                "redis_url": c.get("redis_url") if include_secrets else (mask_secret(c.get("redis_url")) if c.get("redis_url") else None),
                "redis_host": c.get("redis_host", "localhost"),
                "redis_port": c.get("redis_port", 6379),
                "redis_password": redis_pass if include_secrets else mask_secret(redis_pass),
                "redis_password_configured": bool(redis_pass),
                "redis_db": c.get("redis_db", 0),
                "redis_channel": c.get("redis_channel", "melodi:ws:events"),
            },
            "sessions": {
                "grace_period_seconds": c.get("grace_period_seconds", 180),
                "max_connections": c.get("max_connections", 10000),
                "ping_interval_seconds": c.get("ping_interval_seconds", 30),
            },
            "persistence": {
                "config_file": self.storage_file_path,
                "file_exists": os.path.isfile(self.config_file),
            }
        }

    def update_config(self, updates: Dict[str, Any], persist: bool = True) -> Tuple[bool, list[str], bool, str]:
        """
        Applies updates to in-memory configuration and optionally writes to disk.
        Returns:
            (success: bool, updated_keys: list[str], restart_required: bool, error_msg: str)
        """
        if not isinstance(updates, dict):
            return False, [], False, "Payload must be a JSON object"

        # Keys requiring a process restart
        RESTART_KEYS = {"port", "host"}

        updated_keys = []
        restart_required = False

        # Work on a copy first for validation
        candidate = copy.deepcopy(self._config)

        # Allow flat keys or nested structured keys
        flat_updates = {}
        for k, v in updates.items():
            if isinstance(v, dict) and k in ("server", "security", "integration", "pubsub", "sessions"):
                for sub_k, sub_v in v.items():
                    flat_updates[sub_k] = sub_v
            else:
                flat_updates[k] = v

        for key, val in flat_updates.items():
            if key in ("persist", "include_secrets", "restart_required", "file_exists"):
                continue

            if key not in DEFAULT_CONFIG:
                continue

            # Validate types and values
            try:
                if key in ("port", "redis_port", "redis_db", "grace_period_seconds", "melodi_port", "max_connections", "ping_interval_seconds"):
                    val = int(val)
                    if key in ("port", "redis_port", "melodi_port") and not (1 <= val <= 65535):
                        return False, [], False, f"Invalid port range for '{key}': {val}"
                    if key in ("grace_period_seconds", "ping_interval_seconds") and val < 0:
                        return False, [], False, f"'{key}' must be non-negative"
                    if key == "max_connections" and val < 1:
                        return False, [], False, f"'{key}' must be at least 1"

                elif key in ("secret", "redis_host", "redis_channel", "melodi_host", "melodi_platform", "env", "host"):
                    val = str(val).strip()
                    if key == "secret" and not val:
                        return False, [], False, "Secret cannot be empty"

                elif key == "scale_mode":
                    val = str(val).strip().lower()
                    if val in ("scalable", "cluster"):
                        val = "scalable"
                    elif val in ("autonome", "standalone", "local"):
                        val = "autonome"
                    else:
                        return False, [], False, f"Invalid scale_mode '{val}'. Must be 'autonome' or 'scalable'."

                elif key == "log_level":
                    val = str(val).strip().upper()
                    if val not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
                        return False, [], False, f"Invalid log_level '{val}'"

                elif key == "allow_private_internal":
                    val = bool(val)

                elif key == "redis_password":
                    val = str(val).strip() if val else None

                elif key == "redis_url":
                    val = str(val).strip() if val else None

            except (ValueError, TypeError) as exc:
                return False, [], False, f"Invalid value for '{key}': {exc}"

            if candidate.get(key) != val:
                candidate[key] = val
                updated_keys.append(key)
                if key in RESTART_KEYS:
                    restart_required = True

        if not updated_keys:
            return True, [], False, "No settings changed"

        self._config = candidate

        if persist:
            self.save_to_file()

        return True, updated_keys, restart_required, "Configuration updated successfully"

    def reset_to_startup(self) -> None:
        """Resets configuration to initial startup values."""
        self._config = copy.deepcopy(self._initial_startup_config)
        if not getattr(self, "_file_existed_at_startup", False):
            if os.path.exists(self.config_file):
                try:
                    os.remove(self.config_file)
                except Exception:
                    pass
        else:
            self.save_to_file()

    @staticmethod
    async def test_redis_connection(
        host: str = "localhost",
        port: int = 6379,
        password: str | None = None,
        db: int = 0,
        redis_url: str | None = None
    ) -> Tuple[bool, float, str]:
        """
        Tests standalone Redis connection and measures round-trip latency in milliseconds.
        Returns (success: bool, latency_ms: float, message: str)
        """
        start = time.perf_counter()
        try:
            import redis.asyncio as aioredis
        except ImportError:
            return False, 0.0, "Package 'redis' is not installed in the python environment"

        client = None
        try:
            if redis_url:
                client = aioredis.from_url(redis_url, decode_responses=True, socket_timeout=3.0)
            else:
                client = aioredis.Redis(
                    host=host,
                    port=port,
                    password=password if password else None,
                    db=db,
                    decode_responses=True,
                    socket_timeout=3.0
                )
            pong = await client.ping()
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            if pong is True or str(pong).upper() == "PONG":
                return True, elapsed_ms, f"Redis connection successful on {host}:{port} ({pong})"
            return False, elapsed_ms, f"Unexpected Redis reply: {pong}"
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            return False, elapsed_ms, f"Redis connection failed on {host}:{port} : {exc}"
        finally:
            if client:
                try:
                    await client.aclose()
                except Exception:
                    pass

    @staticmethod
    async def test_melodi_connection(host: str = "localhost", port: int = 3000) -> Tuple[bool, float, str]:
        """
        Tests connectivity to the main Melodi ERP HTTP port.
        Returns (success: bool, latency_ms: float, message: str)
        """
        start = time.perf_counter()
        reader = None
        writer = None
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=3.0
            )
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            return True, elapsed_ms, f"Successfully connected to Melodi ERP on {host}:{port}"
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            return False, elapsed_ms, f"Cannot reach Melodi ERP on {host}:{port}: {exc}"
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

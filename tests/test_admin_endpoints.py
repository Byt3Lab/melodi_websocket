import os
import json
import pytest
from main import app, manager, config_mgr, config


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture
def test_app():
    return app


@pytest.fixture(autouse=True)
def cleanup_test_state():
    # Setup: clean slate before test
    if os.path.exists(config_mgr.config_file):
        try:
            os.remove(config_mgr.config_file)
        except Exception:
            pass
    config_mgr.reset_to_startup()
    global config
    config.update(config_mgr.config)
    manager.grace_period_seconds = 180
    manager.active_connections.clear()
    manager.user_sockets.clear()
    manager.channel_sockets.clear()
    manager.sessions.clear()

    yield

    # Teardown: clean slate after test
    manager.active_connections.clear()
    manager.user_sockets.clear()
    manager.channel_sockets.clear()
    manager.sessions.clear()
    config_mgr.reset_to_startup()
    config.update(config_mgr.config)
    manager.grace_period_seconds = 180
    if os.path.exists(config_mgr.config_file):
        try:
            os.remove(config_mgr.config_file)
        except Exception:
            pass


@pytest.mark.anyio
async def test_get_config_unauthorized(test_app):
    client = test_app.test_client()
    res = await client.get("/internal/config")
    assert res.status_code == 401
    data = await res.get_json()
    assert data["status"] == "error"


@pytest.mark.anyio
async def test_get_config_authorized_and_masking(test_app):
    client = test_app.test_client()
    headers = {"X-Internal-Secret": config["secret"]}

    # Without include_secrets
    res = await client.get("/internal/config", headers=headers)
    assert res.status_code == 200
    data = await res.get_json()
    assert data["status"] == "ok"
    assert "config" in data
    assert "node_id" in data
    assert "uptime_seconds" in data
    assert data["config"]["security"]["secret"].startswith("mel...") or data["config"]["security"]["secret"] == "******"

    # With include_secrets
    res_full = await client.get("/internal/config?include_secrets=true", headers=headers)
    assert res_full.status_code == 200
    data_full = await res_full.get_json()
    assert data_full["config"]["security"]["secret"] == config["secret"]


@pytest.mark.anyio
async def test_update_config_dynamic_and_persist(test_app, tmp_path):
    client = test_app.test_client()
    headers = {"X-Internal-Secret": config["secret"]}

    # Update dynamic settings: grace_period_seconds and log_level
    payload = {
        "grace_period_seconds": 99,
        "log_level": "DEBUG",
        "persist": True
    }
    res = await client.post("/internal/config", json=payload, headers=headers)
    assert res.status_code == 200
    data = await res.get_json()
    assert data["status"] == "success"
    assert "grace_period_seconds" in data["updated_keys"]
    assert "log_level" in data["updated_keys"]
    assert data["restart_required"] is False
    assert manager.grace_period_seconds == 99

    # Update a setting requiring restart (port)
    res_port = await client.post("/internal/config", json={"port": 8099}, headers=headers)
    assert res_port.status_code == 200
    data_port = await res_port.get_json()
    assert data_port["restart_required"] is True
    assert "port" in data_port["updated_keys"]

    # Test validation error
    res_err = await client.post("/internal/config", json={"port": 999999}, headers=headers)
    assert res_err.status_code == 400
    data_err = await res_err.get_json()
    assert data_err["status"] == "error"


@pytest.mark.anyio
async def test_test_config_endpoint(test_app):
    client = test_app.test_client()
    headers = {"X-Internal-Secret": config["secret"]}

    # Test redis connection to a non-existent port (should return failure with latency without raising unhandled exception)
    payload_redis = {
        "target": "redis",
        "redis_host": "127.0.0.1",
        "redis_port": 54321
    }
    res = await client.post("/internal/config/test", json=payload_redis, headers=headers)
    assert res.status_code == 200
    data = await res.get_json()
    assert data["target"] == "redis"
    assert "latency_ms" in data
    assert "success" in data

    # Test melodi connection
    payload_melodi = {
        "target": "melodi",
        "melodi_host": "127.0.0.1",
        "melodi_port": 54321
    }
    res_m = await client.post("/internal/config/test", json=payload_melodi, headers=headers)
    assert res_m.status_code == 200
    data_m = await res_m.get_json()
    assert data_m["target"] == "melodi"
    assert "latency_ms" in data_m


@pytest.mark.anyio
async def test_admin_stats_endpoint(test_app):
    client = test_app.test_client()
    headers = {"X-Internal-Secret": config["secret"]}

    res = await client.get("/internal/stats", headers=headers)
    assert res.status_code == 200
    data = await res.get_json()
    assert data["status"] == "ok"
    assert "connections" in data
    assert "messages" in data
    assert "channels" in data
    assert "users" in data
    assert "sessions" in data
    assert "system" in data


@pytest.mark.anyio
async def test_admin_channels_and_clients_and_disconnect(test_app):
    client = test_app.test_client()
    headers = {"X-Internal-Secret": config["secret"]}

    # Connect a simulated websocket client
    async with client.websocket("/ws?token=fake&session_id=sess_admin_test") as ws:
        init_raw = await ws.receive()
        init_data = json.loads(init_raw)
        client_id = init_data["data"]["client_id"]

        # Subscribe client to channel 'admin_test_ch'
        await ws.send(json.dumps({"action": "subscribe", "channel": "admin_test_ch"}))
        await ws.receive()

        # 1. Query channels
        res_ch = await client.get("/internal/admin/channels", headers=headers)
        assert res_ch.status_code == 200
        ch_data = await res_ch.get_json()
        assert ch_data["total_channels"] >= 1
        ch_entry = next((c for c in ch_data["channels"] if c["name"] == "admin_test_ch"), None)
        assert ch_entry is not None
        assert ch_entry["subscriber_count"] >= 1

        # 2. Query clients
        res_cl = await client.get("/internal/admin/clients", headers=headers)
        assert res_cl.status_code == 200
        cl_data = await res_cl.get_json()
        assert cl_data["total_clients"] >= 1
        found = any(c["client_id"] == client_id for c in cl_data["clients"])
        assert found is True

        # Query clients filtered by channel
        res_filt = await client.get("/internal/admin/clients?channel=admin_test_ch", headers=headers)
        assert res_filt.status_code == 200
        filt_data = await res_filt.get_json()
        assert any(c["client_id"] == client_id for c in filt_data["clients"])

        # 3. Disconnect client by client_id
        res_disc = await client.post("/internal/admin/clients/disconnect", json={
            "client_id": client_id,
            "reason": "Administrative kick test"
        }, headers=headers)
        assert res_disc.status_code == 200
        disc_data = await res_disc.get_json()
        assert disc_data["status"] == "success"

        # Verify client received disconnected notification
        disc_msg_raw = await ws.receive()
        disc_msg = json.loads(disc_msg_raw)
        assert disc_msg["event"] == "disconnected"
        assert disc_msg["data"]["reason"] == "Administrative kick test"


@pytest.mark.anyio
async def test_admin_clear_grace_sessions(test_app):
    client = test_app.test_client()
    headers = {"X-Internal-Secret": config["secret"]}

    # Simulate a grace session
    async with client.websocket("/ws?session_id=grace_test_sid") as ws:
        await ws.receive()

    # Disconnected, now session is in grace period
    assert "grace_test_sid" in manager.sessions

    # Clear grace sessions
    res = await client.post("/internal/admin/sessions/clear-grace", headers=headers)
    assert res.status_code == 200
    data = await res.get_json()
    assert data["status"] == "success"
    assert data["cleared_count"] >= 1
    assert "grace_test_sid" not in manager.sessions

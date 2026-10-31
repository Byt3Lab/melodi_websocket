import json
import pytest
from main import app, manager


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.fixture
def test_app():
    return app


@pytest.mark.anyio
async def test_root_health(test_app):
    client = test_app.test_client()
    response = await client.get("/")
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"
    assert data["service"] == "melodi_websocket"


@pytest.mark.anyio
async def test_internal_health(test_app):
    client = test_app.test_client()
    response = await client.get("/internal/health")
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "ok"
    assert "active_connections" in data


@pytest.mark.anyio
async def test_broadcast_unauthorized(test_app):
    client = test_app.test_client()
    response = await client.post("/internal/broadcast", json={"event": "test"})
    assert response.status_code == 401


@pytest.mark.anyio
async def test_broadcast_authorized_success(test_app):
    client = test_app.test_client()
    headers = {"X-Internal-Secret": "melodi_ws_secret_456"}
    payload = {
        "event": "order_created",
        "data": {"order_id": 42}
    }
    response = await client.post("/internal/broadcast", json=payload, headers=headers)
    assert response.status_code == 200
    data = await response.get_json()
    assert data["status"] == "success"
    assert data["event"] == "order_created"
    assert data["recipients_count"] == 0


@pytest.mark.anyio
async def test_internal_health_forbidden_for_public_ip(test_app):
    from main import is_allowed_internal_ip
    assert is_allowed_internal_ip("8.8.8.8") is False
    assert is_allowed_internal_ip("93.184.216.34") is False
    assert is_allowed_internal_ip("172.18.0.2") is True
    assert is_allowed_internal_ip("192.168.1.50") is True
    assert is_allowed_internal_ip("10.0.0.1") is True
    assert is_allowed_internal_ip("127.0.0.1") is True


@pytest.mark.anyio
async def test_websocket_ping_pong_and_channel(test_app):
    client = test_app.test_client()
    async with client.websocket("/ws") as ws:
        # 1. First message on connection should be 'connected'
        init_raw = await ws.receive()
        init_msg = json.loads(init_raw)
        assert init_msg["event"] == "connected"
        assert "client_id" in init_msg["data"]

        # 2. Test Ping / Pong
        await ws.send(json.dumps({"action": "ping"}))
        pong_raw = await ws.receive()
        pong_msg = json.loads(pong_raw)
        assert pong_msg["event"] == "pong"

        # 3. Test Subscribe
        await ws.send(json.dumps({"action": "subscribe", "channel": "inventory"}))
        sub_raw = await ws.receive()
        sub_msg = json.loads(sub_raw)
        assert sub_msg["event"] == "subscribed"
        assert sub_msg["data"]["channel"] == "inventory"

        # 4. Test Receiving a Broadcast Event
        headers = {"X-Internal-Secret": "melodi_ws_secret_456"}
        broadcast_payload = {
            "event": "stock_alert",
            "data": {"sku": "ITEM-123", "qty": 5},
            "channels": ["inventory"]
        }
        post_res = await client.post("/internal/broadcast", json=broadcast_payload, headers=headers)
        assert post_res.status_code == 200
        post_data = await post_res.get_json()
        assert post_data["recipients_count"] == 1

        # Check client received event
        event_raw = await ws.receive()
        event_msg = json.loads(event_raw)
        assert event_msg["event"] == "stock_alert"
        assert event_msg["data"]["sku"] == "ITEM-123"

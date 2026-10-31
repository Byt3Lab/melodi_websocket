import asyncio
import pytest
from pubsub import LocalPubSub, RedisPubSub


@pytest.fixture
def anyio_backend():
    return 'asyncio'


@pytest.mark.anyio
async def test_local_pubsub_lifecycle_and_publish():
    bus = LocalPubSub()
    assert bus.name == "autonome"

    # Connection check must succeed unconditionally
    ok, msg = await bus.check_connection()
    assert ok is True

    received_messages = []

    async def on_message(envelope):
        received_messages.append(envelope)
        return 42

    await bus.start(on_message)

    payload = {
        "origin_node": "node_test_1",
        "message": {"event": "invoice_paid", "data": {"id": 10}},
        "user_ids": [1],
        "channels": None
    }
    result = await bus.publish(payload)
    assert result == 42
    assert len(received_messages) == 1
    assert received_messages[0]["message"]["event"] == "invoice_paid"

    await bus.stop()


@pytest.mark.anyio
async def test_redis_pubsub_fail_fast_when_unreachable():
    # Invalid port to simulate unreachable Redis
    bus = RedisPubSub(host="127.0.0.1", port=59999, password="wrong", channel="test:fail")
    assert bus.name == "scalable"

    # check_connection should fail
    ok, err = await bus.check_connection()
    assert ok is False
    assert len(err) > 0

    # start() must raise ConnectionError to prevent startup
    with pytest.raises(ConnectionError) as exc_info:
        await bus.start(lambda env: None)
    assert "Échec critique de connexion à Redis" in str(exc_info.value)


@pytest.mark.anyio
async def test_redis_pubsub_real_fanout_between_nodes():
    # Connect two distinct RedisPubSub instances to the local test Redis (melodi_redis: 6390, root)
    channel_name = f"test:ws:fanout:{asyncio.get_event_loop().time()}"

    node1 = RedisPubSub(host="127.0.0.1", port=6390, password="root", channel=channel_name)
    node2 = RedisPubSub(host="127.0.0.1", port=6390, password="root", channel=channel_name)

    # 1. Connection check passes
    ok1, msg1 = await node1.check_connection()
    assert ok1 is True, f"Node 1 Redis check failed: {msg1}"

    ok2, msg2 = await node2.check_connection()
    assert ok2 is True, f"Node 2 Redis check failed: {msg2}"

    received_node1 = []
    received_node2 = []

    async def on_node1(env):
        received_node1.append(env)

    async def on_node2(env):
        received_node2.append(env)

    # 2. Start listeners
    await node1.start(on_node1)
    await node2.start(on_node2)

    # Brief pause to allow Redis subscription propagation
    await asyncio.sleep(0.1)

    # 3. Node 1 publishes an envelope
    envelope = {
        "origin_node": "node_1",
        "message": {"event": "cluster_ping", "data": {"seq": 100}},
        "user_ids": None,
        "channels": ["global"]
    }
    subscribers_count = await node1.publish(envelope)
    # Both node1 and node2 should receive the published message
    assert subscribers_count >= 2

    # Wait for async delivery via Redis pubsub loop
    for _ in range(20):
        if len(received_node1) >= 1 and len(received_node2) >= 1:
            break
        await asyncio.sleep(0.05)

    assert len(received_node1) == 1
    assert received_node1[0]["message"]["event"] == "cluster_ping"

    assert len(received_node2) == 1
    assert received_node2[0]["message"]["event"] == "cluster_ping"

    # 4. Clean shutdown
    await node1.stop()
    await node2.stop()

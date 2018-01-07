import pytest

from asgi_redis.core import RedisChannelLayer

TEST_HOSTS = [("localhost", 6379)]


@pytest.mark.asyncio
async def test_send_receive():
    """
    Makes sure we can send a message to a normal channel then receive it.
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS)
    await channel_layer.send(
        "test-channel-1",
        {
            "type": "test.message",
            "text": "Ahoy-hoy!",
        },
    )
    message = await channel_layer.receive("test-channel-1")
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"
    await channel_layer.flush()

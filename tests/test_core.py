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


@pytest.mark.asyncio
async def test_process_local_send_receive():
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS)
    channel_name = channel_layer.new_channel()
    await channel_layer.send(
        channel_name,
        {
            "type": "test.message",
            "text": "Local only please",
        },
    )
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Local only please"
    await channel_layer.flush()


@pytest.mark.asyncio
async def test_reject_bad_client_prefix():
    """
    Makes sure receiving on a non-prefixed local channel is not allowed.
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS)
    with pytest.raises(AssertionError):
        await channel_layer.receive("not-client-prefix!local_part")

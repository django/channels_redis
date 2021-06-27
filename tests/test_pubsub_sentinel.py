import asyncio
import random

import async_timeout
import pytest
from async_generator import async_generator, yield_

from channels_redis.pubsub import RedisPubSubChannelLayer

SENTINEL_MASTER = "sentinel"
TEST_HOSTS = [{"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER}]


@pytest.fixture()
@async_generator
async def channel_layer():
    """
    Channel layer fixture that flushes automatically.
    """
    channel_layer = RedisPubSubChannelLayer(hosts=TEST_HOSTS)
    await yield_(channel_layer)
    await channel_layer.flush()


@pytest.mark.asyncio
async def test_send_receive(channel_layer):
    """
    Makes sure we can send a message to a normal channel then receive it.
    """
    channel = await channel_layer.new_channel()
    await channel_layer.send(channel, {"type": "test.message", "text": "Ahoy-hoy!"})
    message = await channel_layer.receive(channel)
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"


@pytest.mark.asyncio
async def test_multi_send_receive(channel_layer):
    """
    Tests overlapping sends and receives, and ordering.
    """
    channel = await channel_layer.new_channel()
    await channel_layer.send(channel, {"type": "message.1"})
    await channel_layer.send(channel, {"type": "message.2"})
    await channel_layer.send(channel, {"type": "message.3"})
    assert (await channel_layer.receive(channel))["type"] == "message.1"
    assert (await channel_layer.receive(channel))["type"] == "message.2"
    assert (await channel_layer.receive(channel))["type"] == "message.3"


@pytest.mark.asyncio
async def test_groups_basic(channel_layer):
    """
    Tests basic group operation.
    """
    channel_name1 = await channel_layer.new_channel(prefix="test-gr-chan-1")
    channel_name2 = await channel_layer.new_channel(prefix="test-gr-chan-2")
    channel_name3 = await channel_layer.new_channel(prefix="test-gr-chan-3")
    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_discard("test-group", channel_name2)
    await channel_layer.group_send("test-group", {"type": "message.1"})
    # Make sure we get the message on the two channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"
    # Make sure the removed channel did not get the message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name2)


@pytest.mark.asyncio
async def test_groups_same_prefix(channel_layer):
    """
    Tests group_send with multiple channels with same channel prefix
    """
    channel_name1 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name2 = await channel_layer.new_channel(prefix="test-gr-chan")
    channel_name3 = await channel_layer.new_channel(prefix="test-gr-chan")
    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name2))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"


@pytest.mark.asyncio
async def test_random_reset__channel_name(channel_layer):
    """
    Makes sure resetting random seed does not make us reuse channel names.
    """
    random.seed(1)
    channel_name_1 = await channel_layer.new_channel()
    random.seed(1)
    channel_name_2 = await channel_layer.new_channel()

    assert channel_name_1 != channel_name_2

import asyncio
import random

import async_timeout
import pytest
from async_generator import async_generator, yield_

from asgiref.sync import async_to_sync
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
def test_send_receive_sync(channel_layer, event_loop):
    _await = event_loop.run_until_complete
    channel = _await(channel_layer.new_channel())
    async_to_sync(channel_layer.send, force_new_loop=True)(
        channel, {"type": "test.message", "text": "Ahoy-hoy!"}
    )
    message = _await(channel_layer.receive(channel))
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
def test_multi_send_receive_sync(channel_layer, event_loop):
    _await = event_loop.run_until_complete
    channel = _await(channel_layer.new_channel())
    send = async_to_sync(channel_layer.send)
    send(channel, {"type": "message.1"})
    send(channel, {"type": "message.2"})
    send(channel, {"type": "message.3"})
    assert _await(channel_layer.receive(channel))["type"] == "message.1"
    assert _await(channel_layer.receive(channel))["type"] == "message.2"
    assert _await(channel_layer.receive(channel))["type"] == "message.3"


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


@pytest.mark.asyncio
async def test_loop_instance_channel_layer_reference(channel_layer):
    redis_pub_sub_loop_layer = channel_layer._get_layer()

    assert redis_pub_sub_loop_layer.channel_layer == channel_layer


def test_serialize(channel_layer):
    """
    Test default serialization method
    """
    message = {"a": True, "b": None, "c": {"d": []}}
    serialized = channel_layer.serialize(message)
    assert isinstance(serialized, bytes)
    assert serialized == b"\x83\xa1a\xc3\xa1b\xc0\xa1c\x81\xa1d\x90"


def test_deserialize(channel_layer):
    """
    Test default deserialization method
    """
    message = b"\x83\xa1a\xc3\xa1b\xc0\xa1c\x81\xa1d\x90"
    deserialized = channel_layer.deserialize(message)

    assert isinstance(deserialized, dict)
    assert deserialized == {"a": True, "b": None, "c": {"d": []}}


def test_multi_event_loop_garbage_collection(channel_layer):
    """
    Test loop closure layer flushing and garbage collection
    """
    assert len(channel_layer._layers.values()) == 0
    async_to_sync(test_send_receive)(channel_layer)
    assert len(channel_layer._layers.values()) == 0

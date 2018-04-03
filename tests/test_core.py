import asyncio

import async_timeout
import pytest
from async_generator import async_generator, yield_

from asgiref.sync import async_to_sync
from channels_redis.core import ChannelFull, RedisChannelLayer

TEST_HOSTS = [("localhost", 6379)]

MULTIPLE_TEST_HOSTS = [
    "redis://localhost:6379/0",
    "redis://localhost:6379/1",
    "redis://localhost:6379/2",
    "redis://localhost:6379/3",
    "redis://localhost:6379/4",
    "redis://localhost:6379/5",
    "redis://localhost:6379/6",
    "redis://localhost:6379/7",
    "redis://localhost:6379/8",
    "redis://localhost:6379/9",
]


@pytest.fixture()
@async_generator
async def channel_layer():
    """
    Channel layer fixture that flushes automatically.
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS, capacity=3)
    await yield_(channel_layer)
    await channel_layer.flush()


@pytest.fixture()
@async_generator
async def channel_layer_multiple_hosts():
    """
    Channel layer fixture that flushes automatically.
    """
    channel_layer = RedisChannelLayer(hosts=MULTIPLE_TEST_HOSTS, capacity=3)
    await yield_(channel_layer)
    await channel_layer.flush()


@pytest.mark.asyncio
async def test_send_receive(channel_layer):
    """
    Makes sure we can send a message to a normal channel then receive it.
    """
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


@pytest.mark.parametrize("channel_layer", [None])  # Fixture can't handle sync
def test_double_receive(channel_layer):
    """
    Makes sure we can receive from two different event loops using
    process-local channel names.
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS, capacity=3)
    channel_name_1 = async_to_sync(channel_layer.new_channel)()
    channel_name_2 = async_to_sync(channel_layer.new_channel)()
    async_to_sync(channel_layer.send)(channel_name_1, {"type": "test.message.1"})
    async_to_sync(channel_layer.send)(channel_name_2, {"type": "test.message.2"})

    # Make things to listen on the loops
    async def listen1():
        message = await channel_layer.receive(channel_name_1)
        assert message["type"] == "test.message.1"

    async def listen2():
        message = await channel_layer.receive(channel_name_2)
        assert message["type"] == "test.message.2"

    # Run them inside threads
    async_to_sync(listen2)()
    async_to_sync(listen1)()
    # Clean up
    async_to_sync(channel_layer.flush)()


@pytest.mark.asyncio
async def test_send_capacity(channel_layer):
    """
    Makes sure we get ChannelFull when we hit the send capacity
    """
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    await channel_layer.send("test-channel-1", {"type": "test.message"})
    with pytest.raises(ChannelFull):
        await channel_layer.send("test-channel-1", {"type": "test.message"})


@pytest.mark.asyncio
async def test_send_specific_capacity(channel_layer):
    """
    Makes sure we get ChannelFull when we hit the send capacity on a specific channel
    """
    custom_channel_layer = RedisChannelLayer(hosts=TEST_HOSTS, capacity=3, channel_capacity={"one": 1})
    await custom_channel_layer.send("one", {"type": "test.message"})
    with pytest.raises(ChannelFull):
        await custom_channel_layer.send("one", {"type": "test.message"})
    await custom_channel_layer.flush()


@pytest.mark.asyncio
async def test_process_local_send_receive(channel_layer):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    channel_name = await channel_layer.new_channel()
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


@pytest.mark.asyncio
async def test_multi_send_receive(channel_layer):
    """
    Tests overlapping sends and receives, and ordering.
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS)
    await channel_layer.send("test-channel-3", {"type": "message.1"})
    await channel_layer.send("test-channel-3", {"type": "message.2"})
    await channel_layer.send("test-channel-3", {"type": "message.3"})
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.1"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.2"
    assert (await channel_layer.receive("test-channel-3"))["type"] == "message.3"


@pytest.mark.asyncio
async def test_reject_bad_channel(channel_layer):
    """
    Makes sure sending/receiving on an invalic channel name fails.
    """
    with pytest.raises(TypeError):
        await channel_layer.send("=+135!", {"type": "foom"})
    with pytest.raises(TypeError):
        await channel_layer.receive("=+135!")


@pytest.mark.asyncio
async def test_reject_bad_client_prefix(channel_layer):
    """
    Makes sure receiving on a non-prefixed local channel is not allowed.
    """
    with pytest.raises(AssertionError):
        await channel_layer.receive("not-client-prefix!local_part")


@pytest.mark.asyncio
async def test_groups_basic(channel_layer):
    """
    Tests basic group operation.
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS)
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
async def test_groups_channel_full(channel_layer):
    """
    Tests that group_send ignores ChannelFull
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS)
    await channel_layer.group_add("test-group", "test-gr-chan-1")
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})


@pytest.mark.asyncio
async def test_groups_multiple_hosts(channel_layer_multiple_hosts):
    """
    Tests advanced group operation with multiple hosts.
    """
    channel_layer = RedisChannelLayer(hosts=MULTIPLE_TEST_HOSTS, capacity=100)
    channel_name1 = await channel_layer.new_channel(prefix="channel1")
    channel_name2 = await channel_layer.new_channel(prefix="channel2")
    channel_name3 = await channel_layer.new_channel(prefix="channel3")
    await channel_layer.group_add("test-group", channel_name1)
    await channel_layer.group_add("test-group", channel_name2)
    await channel_layer.group_add("test-group", channel_name3)
    await channel_layer.group_discard("test-group", channel_name2)
    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    async with async_timeout.timeout(1):
        assert (await channel_layer.receive(channel_name1))["type"] == "message.1"
        assert (await channel_layer.receive(channel_name3))["type"] == "message.1"

    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name2)


@pytest.mark.parametrize("num_channels,timeout", [
    (1, 1),  # Edge cases - make sure we can send to a single channel
    (10, 1),
    (100, 10),
])
@pytest.mark.asyncio
async def test_groups_multiple_hosts_performance(
    channel_layer_multiple_hosts, num_channels, timeout
):
    """
    Tests advanced group operation: can send efficiently to multiple channels
    with multiple hosts within a certain timeout
    """
    channel_layer = RedisChannelLayer(hosts=MULTIPLE_TEST_HOSTS, capacity=100)

    channels = []
    for i in range(0, num_channels):
        channel = await channel_layer.new_channel(prefix="channel%s" % i)
        await channel_layer.group_add("test-group", channel)
        channels.append(channel)

    async with async_timeout.timeout(timeout):
        await channel_layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message all the channels
    async with async_timeout.timeout(timeout):
        for channel in channels:
            assert (await channel_layer.receive(channel))["type"] == "message.1"

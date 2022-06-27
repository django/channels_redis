import asyncio
import random

import async_timeout
import pytest
from async_generator import async_generator, yield_

from asgiref.sync import async_to_sync
from channels_redis.core import ChannelFull, RedisChannelLayer

SENTINEL_MASTER = "sentinel"
TEST_HOSTS = [{"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER}]

MULTIPLE_TEST_HOSTS = [
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 0},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 1},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 2},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 3},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 4},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 5},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 6},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 7},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 8},
    {"sentinels": [("localhost", 26379)], "master_name": SENTINEL_MASTER, "db": 9},
]


async def send_three_messages_with_delay(channel_name, channel_layer, delay):
    await channel_layer.send(channel_name, {"type": "test.message", "text": "First!"})

    await asyncio.sleep(delay)

    await channel_layer.send(channel_name, {"type": "test.message", "text": "Second!"})

    await asyncio.sleep(delay)

    await channel_layer.send(channel_name, {"type": "test.message", "text": "Third!"})


async def group_send_three_messages_with_delay(group_name, channel_layer, delay):
    await channel_layer.group_send(
        group_name, {"type": "test.message", "text": "First!"}
    )

    await asyncio.sleep(delay)

    await channel_layer.group_send(
        group_name, {"type": "test.message", "text": "Second!"}
    )

    await asyncio.sleep(delay)

    await channel_layer.group_send(
        group_name, {"type": "test.message", "text": "Third!"}
    )


@pytest.fixture()
@async_generator
async def channel_layer():
    """
    Channel layer fixture that flushes automatically.
    """
    channel_layer = RedisChannelLayer(
        hosts=TEST_HOSTS, capacity=3, channel_capacity={"tiny": 1}
    )
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
        "test-channel-1", {"type": "test.message", "text": "Ahoy-hoy!"}
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

    # Aioredis connections can't be used from different event loops, so
    # send and close need to be done in the same async_to_sync call.
    async def send_and_close(*args, **kwargs):
        await channel_layer.send(*args, **kwargs)
        await channel_layer.close_pools()

    channel_name_1 = async_to_sync(channel_layer.new_channel)()
    channel_name_2 = async_to_sync(channel_layer.new_channel)()
    async_to_sync(send_and_close)(channel_name_1, {"type": "test.message.1"})
    async_to_sync(send_and_close)(channel_name_2, {"type": "test.message.2"})

    # Make things to listen on the loops
    async def listen1():
        message = await channel_layer.receive(channel_name_1)
        assert message["type"] == "test.message.1"
        await channel_layer.close_pools()

    async def listen2():
        message = await channel_layer.receive(channel_name_2)
        assert message["type"] == "test.message.2"
        await channel_layer.close_pools()

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
    custom_channel_layer = RedisChannelLayer(
        hosts=TEST_HOSTS,
        capacity=3,
        channel_capacity={"one": 1},
    )
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
        channel_name, {"type": "test.message", "text": "Local only please"}
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
    await channel_layer.flush()


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
    await channel_layer.flush()


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
    await channel_layer.flush()


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

    await channel_layer.flush()


@pytest.mark.asyncio
async def test_groups_same_prefix(channel_layer):
    """
    Tests group_send with multiple channels with same channel prefix
    """
    channel_layer = RedisChannelLayer(hosts=TEST_HOSTS)
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

    await channel_layer.flush()


@pytest.mark.parametrize(
    "num_channels,timeout",
    [
        (1, 1),  # Edge cases - make sure we can send to a single channel
        (10, 1),
        (100, 10),
    ],
)
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

    await channel_layer.flush()


@pytest.mark.asyncio
async def test_group_send_capacity(channel_layer, caplog):
    """
    Makes sure we dont group_send messages to channels that are over capacity.
    Make sure number of channels with full capacity are logged as an exception to help debug errors.
    """

    channel = await channel_layer.new_channel()
    await channel_layer.group_add("test-group", channel)

    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.2"})
    await channel_layer.group_send("test-group", {"type": "message.3"})
    await channel_layer.group_send("test-group", {"type": "message.4"})

    # We should receive the first 3 messages
    assert (await channel_layer.receive(channel))["type"] == "message.1"
    assert (await channel_layer.receive(channel))["type"] == "message.2"
    assert (await channel_layer.receive(channel))["type"] == "message.3"

    # Make sure we do NOT receive message 4
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel)

    # Make sure number of channels over capacity are logged
    for record in caplog.records:
        assert record.levelname == "INFO"
        assert (
            record.getMessage() == "1 of 1 channels over capacity in group test-group"
        )


@pytest.mark.asyncio
async def test_group_send_capacity_multiple_channels(channel_layer, caplog):
    """
    Makes sure we dont group_send messages to channels that are over capacity
    Make sure number of channels with full capacity are logged as an exception to help debug errors.
    """

    channel_1 = await channel_layer.new_channel()
    channel_2 = await channel_layer.new_channel(prefix="channel_2")
    await channel_layer.group_add("test-group", channel_1)
    await channel_layer.group_add("test-group", channel_2)

    # Let's put channel_2 over capacity
    await channel_layer.send(channel_2, {"type": "message.0"})

    await channel_layer.group_send("test-group", {"type": "message.1"})
    await channel_layer.group_send("test-group", {"type": "message.2"})
    await channel_layer.group_send("test-group", {"type": "message.3"})

    # Channel_1 should receive all 3 group messages
    assert (await channel_layer.receive(channel_1))["type"] == "message.1"
    assert (await channel_layer.receive(channel_1))["type"] == "message.2"
    assert (await channel_layer.receive(channel_1))["type"] == "message.3"

    # Channel_2 should receive the first message + 2 group messages
    assert (await channel_layer.receive(channel_2))["type"] == "message.0"
    assert (await channel_layer.receive(channel_2))["type"] == "message.1"
    assert (await channel_layer.receive(channel_2))["type"] == "message.2"

    # Make sure channel_2 does not receive the 3rd group message
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_2)

    # Make sure number of channels over capacity are logged
    for record in caplog.records:
        assert record.levelname == "INFO"
        assert (
            record.getMessage() == "1 of 2 channels over capacity in group test-group"
        )


@pytest.mark.asyncio
async def test_receive_cancel(channel_layer):
    """
    Makes sure we can cancel a receive without blocking
    """
    channel_layer = RedisChannelLayer(capacity=30)
    channel = await channel_layer.new_channel()
    delay = 0
    while delay < 0.01:
        await channel_layer.send(channel, {"type": "test.message", "text": "Ahoy-hoy!"})

        task = asyncio.ensure_future(channel_layer.receive(channel))
        await asyncio.sleep(delay)
        task.cancel()
        delay += 0.0001

        try:
            await asyncio.wait_for(task, None)
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_random_reset__channel_name(channel_layer):
    """
    Makes sure resetting random seed does not make us reuse channel names.
    """

    channel_layer = RedisChannelLayer()
    random.seed(1)
    channel_name_1 = await channel_layer.new_channel()
    random.seed(1)
    channel_name_2 = await channel_layer.new_channel()

    assert channel_name_1 != channel_name_2


@pytest.mark.asyncio
async def test_random_reset__client_prefix(channel_layer):
    """
    Makes sure resetting random seed does not make us reuse client_prefixes.
    """

    random.seed(1)
    channel_layer_1 = RedisChannelLayer()
    random.seed(1)
    channel_layer_2 = RedisChannelLayer()
    assert channel_layer_1.client_prefix != channel_layer_2.client_prefix


@pytest.mark.asyncio
async def test_message_expiry__earliest_message_expires(channel_layer):
    expiry = 3
    delay = 2
    channel_layer = RedisChannelLayer(expiry=expiry)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(
        send_three_messages_with_delay(channel_name, channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)


@pytest.mark.asyncio
async def test_message_expiry__all_messages_under_expiration_time(channel_layer):
    expiry = 3
    delay = 1
    channel_layer = RedisChannelLayer(expiry=expiry)
    channel_name = await channel_layer.new_channel()

    task = asyncio.ensure_future(
        send_three_messages_with_delay(channel_name, channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # expiry = 3, total delay under 3, all messages there
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"


@pytest.mark.asyncio
async def test_message_expiry__group_send(channel_layer):
    expiry = 3
    delay = 2
    channel_layer = RedisChannelLayer(expiry=expiry)
    channel_name = await channel_layer.new_channel()

    await channel_layer.group_add("test-group", channel_name)

    task = asyncio.ensure_future(
        group_send_three_messages_with_delay("test-group", channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_name)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no third message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_name)


@pytest.mark.asyncio
async def test_message_expiry__group_send__one_channel_expires_message(channel_layer):
    expiry = 3
    delay = 1

    channel_layer = RedisChannelLayer(expiry=expiry)
    channel_1 = await channel_layer.new_channel()
    channel_2 = await channel_layer.new_channel(prefix="channel_2")

    await channel_layer.group_add("test-group", channel_1)
    await channel_layer.group_add("test-group", channel_2)

    # Let's give channel_1 one additional message and then sleep
    await channel_layer.send(channel_1, {"type": "test.message", "text": "Zero!"})
    await asyncio.sleep(2)

    task = asyncio.ensure_future(
        group_send_three_messages_with_delay("test-group", channel_layer, delay)
    )
    await asyncio.wait_for(task, None)

    # message Zero! was sent about 2 + 1 + 1 seconds ago and it should have expired
    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_1)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"

    # Make sure there's no fourth message even out of order
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(1):
            await channel_layer.receive(channel_1)

    # channel_2 should receive all three messages from group_send
    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "First!"

    # the first message should have expired, we should only see the second message and the third
    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "Second!"

    message = await channel_layer.receive(channel_2)
    assert message["type"] == "test.message"
    assert message["text"] == "Third!"


def test_default_group_key_format():
    channel_layer = RedisChannelLayer()
    group_name = channel_layer._group_key("test_group")
    assert group_name == b"asgi:group:test_group"


def test_custom_group_key_format():
    channel_layer = RedisChannelLayer(prefix="test_prefix")
    group_name = channel_layer._group_key("test_group")
    assert group_name == b"test_prefix:group:test_group"


def test_receive_buffer_respects_capacity():
    channel_layer = RedisChannelLayer()
    buff = channel_layer.receive_buffer["test-group"]
    for i in range(10000):
        buff.put_nowait(i)

    capacity = 100
    assert channel_layer.capacity == capacity
    assert buff.full() is True
    assert buff.qsize() == capacity
    messages = [buff.get_nowait() for _ in range(capacity)]
    assert list(range(9900, 10000)) == messages


def test_serialize():
    """
    Test default serialization method
    """
    message = {"a": True, "b": None, "c": {"d": []}}
    channel_layer = RedisChannelLayer()
    serialized = channel_layer.serialize(message)
    assert isinstance(serialized, bytes)
    assert serialized[12:] == b"\x83\xa1a\xc3\xa1b\xc0\xa1c\x81\xa1d\x90"


def test_deserialize():
    """
    Test default deserialization method
    """
    message = b"Q\x0c\xbb?Q\xbc\xe3|D\xfd9\x00\x83\xa1a\xc3\xa1b\xc0\xa1c\x81\xa1d\x90"
    channel_layer = RedisChannelLayer()
    deserialized = channel_layer.deserialize(message)

    assert isinstance(deserialized, dict)
    assert deserialized == {"a": True, "b": None, "c": {"d": []}}

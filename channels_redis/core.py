import asyncio
import base64
import binascii
import collections
import functools
import hashlib
import itertools
import logging
import random
import sys
import time
import types
import uuid

import aioredis
import msgpack

from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer

logger = logging.getLogger(__name__)

AIOREDIS_VERSION = tuple(map(int, aioredis.__version__.split(".")))


def _wrap_close(loop, pool):
    """
    Decorate an event loop's close method with our own.
    """
    original_impl = loop.close

    def _wrapper(self, *args, **kwargs):
        # If the event loop was closed, there's nothing we can do anymore.
        if not self.is_closed():
            self.run_until_complete(pool.close_loop(self))
        # Restore the original close() implementation after we're done.
        self.close = original_impl
        return self.close(*args, **kwargs)

    loop.close = types.MethodType(_wrapper, loop)


class ConnectionPool:
    """
    Connection pool manager for the channel layer.
    It manages a set of connections for the given host specification and
    taking into account asyncio event loops.
    """

    def __init__(self, host):
        self.host = host.copy()
        self.master_name = self.host.pop("master_name", None)
        self.conn_map = {}
        self.sentinel_map = {}
        self.in_use = {}

    def _ensure_loop(self, loop):
        """
        Get connection list for the specified loop.
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        if loop not in self.conn_map:
            # Swap the loop's close method with our own so we get
            # a chance to do some cleanup.
            _wrap_close(loop, self)
            self.conn_map[loop] = []

        return self.conn_map[loop], loop

    async def create_conn(self, loop):
        # One connection per pool since we are emulating a single connection
        kwargs = {"minsize": 1, "maxsize": 1, **self.host}
        if not (sys.version_info >= (3, 8, 0) and AIOREDIS_VERSION >= (1, 3, 1)):
            kwargs["loop"] = loop
        if self.master_name is None:
            return await aioredis.create_redis_pool(**kwargs)
        else:
            kwargs = {"timeout": 2, **kwargs}  # aioredis default is way too low
            sentinel = await aioredis.sentinel.create_sentinel(**kwargs)
            conn = sentinel.master_for(self.master_name)
            self.sentinel_map[conn] = sentinel
            return conn

    async def pop(self, loop=None):
        """
        Get a connection for the given identifier and loop.
        """
        conns, loop = self._ensure_loop(loop)
        if not conns:
            conn = await self.create_conn(loop)
            conns.append(conn)
        conn = conns.pop()
        if conn.closed:
            conn = await self.pop(loop=loop)
            return conn
        self.in_use[conn] = loop
        return conn

    def push(self, conn):
        """
        Return a connection to the pool.
        """
        loop = self.in_use[conn]
        del self.in_use[conn]
        if loop is not None:
            conns, _ = self._ensure_loop(loop)
            conns.append(conn)

    async def conn_error(self, conn):
        """
        Handle a connection that produced an error.
        """
        await self._close_conn(conn)
        del self.in_use[conn]

    def reset(self):
        """
        Clear all connections from the pool.
        """
        self.conn_map = {}
        self.sentinel_map = {}
        self.in_use = {}

    async def _close_conn(self, conn, sentinel_map=None):
        if sentinel_map is None:
            sentinel_map = self.sentinel_map
        if conn in sentinel_map:
            sentinel_map[conn].close()
            await sentinel_map[conn].wait_closed()
            del sentinel_map[conn]
        conn.close()
        await conn.wait_closed()

    async def close_loop(self, loop):
        """
        Close all connections owned by the pool on the given loop.
        """
        if loop in self.conn_map:
            for conn in self.conn_map[loop]:
                await self._close_conn(conn)
            del self.conn_map[loop]

        for k, v in self.in_use.items():
            if v is loop:
                await self._close_conn(k)
                self.in_use[k] = None

    async def close(self):
        """
        Close all connections owned by the pool.
        """
        conn_map = self.conn_map
        sentinel_map = self.sentinel_map
        in_use = self.in_use
        self.reset()
        for conns in conn_map.values():
            for conn in conns:
                await self._close_conn(conn, sentinel_map)
        for conn in in_use:
            await self._close_conn(conn, sentinel_map)


class ChannelLock:
    """
    Helper class for per-channel locking.

    Once a lock is released and has no waiters, it will also be deleted,
    to mitigate multi-event loop problems.
    """

    def __init__(self):
        self.locks = collections.defaultdict(asyncio.Lock)
        self.wait_counts = collections.defaultdict(int)

    async def acquire(self, channel):
        """
        Acquire the lock for the given channel.
        """
        self.wait_counts[channel] += 1
        return await self.locks[channel].acquire()

    def locked(self, channel):
        """
        Return ``True`` if the lock for the given channel is acquired.
        """
        return self.locks[channel].locked()

    def release(self, channel):
        """
        Release the lock for the given channel.
        """
        self.locks[channel].release()
        self.wait_counts[channel] -= 1
        if self.wait_counts[channel] < 1:
            del self.locks[channel]
            del self.wait_counts[channel]


class UnsupportedRedis(Exception):
    pass


class BoundedQueue(asyncio.Queue):
    def put_nowait(self, item):
        if self.full():
            # see: https://github.com/django/channels_redis/issues/212
            # if we actually get into this code block, it likely means that
            # this specific consumer has stopped reading
            # if we get into this code block, it's better to drop messages
            # that exceed the channel layer capacity than to continue to
            # malloc() forever
            self.get_nowait()
        return super(BoundedQueue, self).put_nowait(item)


class RedisChannelLayer(BaseChannelLayer):
    """
    Redis channel layer.

    It routes all messages into remote Redis server. Support for
    sharding among different Redis installations and message
    encryption are provided.
    """

    brpop_timeout = 5

    def __init__(
        self,
        hosts=None,
        prefix="asgi",
        expiry=60,
        group_expiry=86400,
        capacity=100,
        channel_capacity=None,
        symmetric_encryption_keys=None,
    ):
        # Store basic information
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = self.compile_capacities(channel_capacity or {})
        self.prefix = prefix
        assert isinstance(self.prefix, str), "Prefix must be unicode"
        # Configure the host objects
        self.hosts = self.decode_hosts(hosts)
        self.ring_size = len(self.hosts)
        # Cached redis connection pools and the event loop they are from
        self.pools = [ConnectionPool(host) for host in self.hosts]
        # Normal channels choose a host index by cycling through the available hosts
        self._receive_index_generator = itertools.cycle(range(len(self.hosts)))
        self._send_index_generator = itertools.cycle(range(len(self.hosts)))
        # Decide on a unique client prefix to use in ! sections
        self.client_prefix = uuid.uuid4().hex
        # Set up any encryption objects
        self._setup_encryption(symmetric_encryption_keys)
        # Number of coroutines trying to receive right now
        self.receive_count = 0
        # The receive lock
        self.receive_lock = None
        # Event loop they are trying to receive on
        self.receive_event_loop = None
        # Buffered messages by process-local channel name
        self.receive_buffer = collections.defaultdict(
            functools.partial(BoundedQueue, self.capacity)
        )
        # Detached channel cleanup tasks
        self.receive_cleaners = []
        # Per-channel cleanup locks to prevent a receive starting and moving
        # a message back into the main queue before its cleanup has completed
        self.receive_clean_locks = ChannelLock()

    def decode_hosts(self, hosts):
        """
        Takes the value of the "hosts" argument passed to the class and returns
        a list of kwargs to use for the Redis connection constructor.
        """
        # If no hosts were provided, return a default value
        if not hosts:
            return [{"address": ("localhost", 6379)}]
        # If they provided just a string, scold them.
        if isinstance(hosts, (str, bytes)):
            raise ValueError(
                "You must pass a list of Redis hosts, even if there is only one."
            )

        # Decode each hosts entry into a kwargs dict
        result = []
        for entry in hosts:
            if isinstance(entry, dict):
                result.append(entry)
            else:
                result.append({"address": entry})
        return result

    def _setup_encryption(self, symmetric_encryption_keys):
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, (str, bytes)):
                raise ValueError(
                    "symmetric_encryption_keys must be a list of possible keys"
                )
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError(
                    "Cannot run with encryption without 'cryptography' installed."
                )
            sub_fernets = [self.make_fernet(key) for key in symmetric_encryption_keys]
            self.crypter = MultiFernet(sub_fernets)
        else:
            self.crypter = None

    ### Channel layer API ###

    extensions = ["groups", "flush"]

    async def send(self, channel, message):
        """
        Send a message onto a (general or specific) channel.
        """
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # Make sure the message does not contain reserved keys
        assert "__asgi_channel__" not in message
        # If it's a process-local channel, strip off local part and stick full name in message
        channel_non_local_name = channel
        if "!" in channel:
            message = dict(message.items())
            message["__asgi_channel__"] = channel
            channel_non_local_name = self.non_local_name(channel)
        # Write out message into expiring key (avoids big items in list)
        channel_key = self.prefix + channel_non_local_name
        # Pick a connection to the right server - consistent for specific
        # channels, random for general channels
        if "!" in channel:
            index = self.consistent_hash(channel)
        else:
            index = next(self._send_index_generator)
        async with self.connection(index) as connection:
            # Discard old messages based on expiry
            await connection.zremrangebyscore(
                channel_key, min=0, max=int(time.time()) - int(self.expiry)
            )

            # Check the length of the list before send
            # This can allow the list to leak slightly over capacity, but that's fine.
            if await connection.zcount(channel_key) >= self.get_capacity(channel):
                raise ChannelFull()

            # Push onto the list then set it to expire in case it's not consumed
            await connection.zadd(channel_key, time.time(), self.serialize(message))
            await connection.expire(channel_key, int(self.expiry))

    def _backup_channel_name(self, channel):
        """
        Construct the key used as a backup queue for the given channel.
        """
        return channel + "$inflight"

    async def _brpop_with_clean(self, index, channel, timeout):
        """
        Perform a Redis BRPOP and manage the backup processing queue.
        In case of cancellation, make sure the message is not lost.
        """
        # The script will pop messages from the processing queue and push them in front
        # of the main message queue in the proper order; BRPOP must *not* be called
        # because that would deadlock the server
        cleanup_script = """
            local backed_up = redis.call('ZRANGE', ARGV[2], 0, -1, 'WITHSCORES')
            for i = #backed_up, 1, -2 do
                redis.call('ZADD', ARGV[1], backed_up[i], backed_up[i - 1])
            end
            redis.call('DEL', ARGV[2])
        """
        backup_queue = self._backup_channel_name(channel)
        async with self.connection(index) as connection:
            # Cancellation here doesn't matter, we're not doing anything destructive
            # and the script executes atomically...
            await connection.eval(cleanup_script, keys=[], args=[channel, backup_queue])
            # ...and it doesn't matter here either, the message will be safe in the backup.
            result = await connection.bzpopmin(channel, timeout=timeout)

            if result is not None:
                _, member, timestamp = result
                await connection.zadd(backup_queue, float(timestamp), member)
            else:
                member = None

            return member

    async def _clean_receive_backup(self, index, channel):
        """
        Pop the oldest message off the channel backup queue.
        The result isn't interesting as it was already processed.
        """
        async with self.connection(index) as connection:
            await connection.zpopmin(self._backup_channel_name(channel))

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, the first waiter
        will be given the message when it arrives.
        """
        # Make sure the channel name is valid then get the non-local part
        # and thus its index
        assert self.valid_channel_name(channel)
        if "!" in channel:
            real_channel = self.non_local_name(channel)
            assert real_channel.endswith(
                self.client_prefix + "!"
            ), "Wrong client prefix"
            # Enter receiving section
            loop = asyncio.get_event_loop()
            self.receive_count += 1
            try:
                if self.receive_count == 1:
                    # If we're the first coroutine in, create the receive lock!
                    self.receive_lock = asyncio.Lock()
                    self.receive_event_loop = loop
                else:
                    # Otherwise, check our event loop matches
                    if self.receive_event_loop != loop:
                        raise RuntimeError(
                            "Two event loops are trying to receive() on one channel layer at once!"
                        )

                # Wait for our message to appear
                message = None
                while self.receive_buffer[channel].empty():
                    tasks = [
                        self.receive_lock.acquire(),
                        self.receive_buffer[channel].get(),
                    ]
                    tasks = [asyncio.ensure_future(task) for task in tasks]
                    try:
                        done, pending = await asyncio.wait(
                            tasks, return_when=asyncio.FIRST_COMPLETED
                        )
                        for task in pending:
                            # Cancel all pending tasks.
                            task.cancel()
                    except asyncio.CancelledError:
                        # Ensure all tasks are cancelled if we are cancelled.
                        # Also see: https://bugs.python.org/issue23859
                        del self.receive_buffer[channel]
                        for task in tasks:
                            if not task.cancel():
                                assert task.done()
                                if task.result() is True:
                                    self.receive_lock.release()

                        raise

                    message, token, exception = None, None, None
                    for task in done:
                        try:
                            result = task.result()
                        except BaseException as error:  # NOQA
                            # We should not propagate exceptions immediately as otherwise this may cause
                            # the lock to be held and never be released.
                            exception = error
                            continue

                        if result is True:
                            token = result
                        else:
                            assert isinstance(result, dict)
                            message = result

                    if message or exception:
                        if token:
                            # We will not be receving as we already have the message.
                            self.receive_lock.release()

                        if exception:
                            raise exception
                        else:
                            break
                    else:
                        assert token

                        # We hold the receive lock, receive and then release it.
                        try:
                            # There is no interruption point from when the message is
                            # unpacked in receive_single to when we get back here, so
                            # the following lines are essentially atomic.
                            message_channel, message = await self.receive_single(
                                real_channel
                            )
                            if type(message_channel) is list:
                                for chan in message_channel:
                                    self.receive_buffer[chan].put_nowait(message)
                            else:
                                self.receive_buffer[message_channel].put_nowait(message)
                            message = None
                        except Exception:
                            del self.receive_buffer[channel]
                            raise
                        finally:
                            self.receive_lock.release()

                # We know there's a message available, because there
                # couldn't have been any interruption between empty() and here
                if message is None:
                    message = self.receive_buffer[channel].get_nowait()

                if self.receive_buffer[channel].empty():
                    del self.receive_buffer[channel]
                return message

            finally:
                self.receive_count -= 1
                # If we were the last out, drop the receive lock
                if self.receive_count == 0:
                    assert not self.receive_lock.locked()
                    self.receive_lock = None
                    self.receive_event_loop = None
        else:
            # Do a plain direct receive
            return (await self.receive_single(channel))[1]

    async def receive_single(self, channel):
        """
        Receives a single message off of the channel and returns it.
        """
        # Check channel name
        assert self.valid_channel_name(channel, receive=True), "Channel name invalid"
        # Work out the connection to use
        if "!" in channel:
            assert channel.endswith("!")
            index = self.consistent_hash(channel)
        else:
            index = next(self._receive_index_generator)

        channel_key = self.prefix + channel
        content = None
        await self.receive_clean_locks.acquire(channel_key)
        try:
            while content is None:
                # Nothing is lost here by cancellations, messages will still
                # be in the backup queue.
                content = await self._brpop_with_clean(
                    index, channel_key, timeout=self.brpop_timeout
                )

            # Fire off a task to clean the message from its backup queue.
            # Per-channel locking isn't needed, because the backup is a queue
            # and additionally, we don't care about the order; all processed
            # messages need to be removed, no matter if the current one is
            # removed after the next one.
            # NOTE: Duplicate messages will be received eventually if any
            # of these cleaners are cancelled.
            cleaner = asyncio.ensure_future(
                self._clean_receive_backup(index, channel_key)
            )
            self.receive_cleaners.append(cleaner)

            def _cleanup_done(cleaner):
                self.receive_cleaners.remove(cleaner)
                self.receive_clean_locks.release(channel_key)

            cleaner.add_done_callback(_cleanup_done)

        except BaseException:
            self.receive_clean_locks.release(channel_key)
            raise

        # Message decode
        message = self.deserialize(content)
        # TODO: message expiry?
        # If there is a full channel name stored in the message, unpack it.
        if "__asgi_channel__" in message:
            channel = message["__asgi_channel__"]
            del message["__asgi_channel__"]
        return channel, message

    async def new_channel(self, prefix="specific"):
        """
        Returns a new channel name that can be used by something in our
        process as a specific channel.
        """
        return "%s.%s!%s" % (
            prefix,
            self.client_prefix,
            uuid.uuid4().hex,
        )

    ### Flush extension ###

    async def flush(self):
        """
        Deletes all messages and groups on all shards.
        """
        # Make sure all channel cleaners have finished before removing
        # keys from under their feet.
        await self.wait_received()

        # Lua deletion script
        delete_prefix = """
            local keys = redis.call('keys', ARGV[1])
            for i=1,#keys,5000 do
                redis.call('del', unpack(keys, i, math.min(i+4999, #keys)))
            end
        """
        # Go through each connection and remove all with prefix
        for i in range(self.ring_size):
            async with self.connection(i) as connection:
                await connection.eval(delete_prefix, keys=[], args=[self.prefix + "*"])
        # Now clear the pools as well
        await self.close_pools()

    async def close_pools(self):
        """
        Close all connections in the event loop pools.
        """
        # Flush all cleaners, in case somebody just wanted to close the
        # pools without flushing first.
        await self.wait_received()

        for pool in self.pools:
            await pool.close()

    async def wait_received(self):
        """
        Wait for all channel cleanup functions to finish.
        """
        if self.receive_cleaners:
            await asyncio.wait(self.receive_cleaners[:])

    ### Groups extension ###

    async def group_add(self, group, channel):
        """
        Adds the channel name to a group.
        """
        # Check the inputs
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"
        # Get a connection to the right shard
        group_key = self._group_key(group)
        async with self.connection(self.consistent_hash(group)) as connection:
            # Add to group sorted set with creation time as timestamp
            await connection.zadd(group_key, time.time(), channel)
            # Set expiration to be group_expiry, since everything in
            # it at this point is guaranteed to expire before that
            await connection.expire(group_key, self.group_expiry)

    async def group_discard(self, group, channel):
        """
        Removes the channel from the named group if it is in the group;
        does nothing otherwise (does not error)
        """
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"
        key = self._group_key(group)
        async with self.connection(self.consistent_hash(group)) as connection:
            await connection.zrem(key, channel)

    async def group_send(self, group, message):
        """
        Sends a message to the entire group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        # Retrieve list of all channel names
        key = self._group_key(group)
        async with self.connection(self.consistent_hash(group)) as connection:
            # Discard old channels based on group_expiry
            await connection.zremrangebyscore(
                key, min=0, max=int(time.time()) - self.group_expiry
            )

            channel_names = [
                x.decode("utf8") for x in await connection.zrange(key, 0, -1)
            ]

        (
            connection_to_channel_keys,
            channel_keys_to_message,
            channel_keys_to_capacity,
        ) = self._map_channel_keys_to_connection(channel_names, message)

        for connection_index, channel_redis_keys in connection_to_channel_keys.items():
            # Discard old messages based on expiry
            pipe = connection.pipeline()
            for key in channel_redis_keys:
                pipe.zremrangebyscore(
                    key, min=0, max=int(time.time()) - int(self.expiry)
                )
            await pipe.execute()

            # Create a LUA script specific for this connection.
            # Make sure to use the message specific to this channel, it is
            # stored in channel_to_message dict and contains the
            # __asgi_channel__ key.

            group_send_lua = """
                local over_capacity = 0
                local current_time = ARGV[#ARGV - 1]
                local expiry = ARGV[#ARGV]
                for i=1,#KEYS do
                    if redis.call('ZCOUNT', KEYS[i], '-inf', '+inf') < tonumber(ARGV[i + #KEYS]) then
                        redis.call('ZADD', KEYS[i], current_time, ARGV[i])
                        redis.call('EXPIRE', KEYS[i], expiry)
                    else
                        over_capacity = over_capacity + 1
                    end
                end
                return over_capacity
            """

            # We need to filter the messages to keep those related to the connection
            args = [
                channel_keys_to_message[channel_key]
                for channel_key in channel_redis_keys
            ]

            # We need to send the capacity for each channel
            args += [
                channel_keys_to_capacity[channel_key]
                for channel_key in channel_redis_keys
            ]

            args += [time.time(), self.expiry]

            # channel_keys does not contain a single redis key more than once
            async with self.connection(connection_index) as connection:
                channels_over_capacity = await connection.eval(
                    group_send_lua, keys=channel_redis_keys, args=args
                )
                if channels_over_capacity > 0:
                    logger.info(
                        "%s of %s channels over capacity in group %s",
                        channels_over_capacity,
                        len(channel_names),
                        group,
                    )

    def _map_channel_to_connection(self, channel_names, message):
        """
        For a list of channel names, bucket each one to a dict keyed by the
        connection index
        Also for each channel create a message specific to that channel, adding
        the __asgi_channel__ key to the message
        We also return a mapping from channel names to their corresponding Redis
        keys, and a mapping of channels to their capacity
        """
        connection_to_channels = collections.defaultdict(list)
        channel_to_message = dict()
        channel_to_capacity = dict()
        channel_to_key = dict()

        for channel in channel_names:
            channel_non_local_name = channel
            if "!" in channel:
                message = dict(message.items())
                message["__asgi_channel__"] = channel
                channel_non_local_name = self.non_local_name(channel)
            channel_key = self.prefix + channel_non_local_name
            idx = self.consistent_hash(channel_non_local_name)
            connection_to_channels[idx].append(channel_key)
            channel_to_capacity[channel] = self.get_capacity(channel)
            channel_to_message[channel] = self.serialize(message)
            # We build a
            channel_to_key[channel] = channel_key

        return (
            connection_to_channels,
            channel_to_message,
            channel_to_capacity,
            channel_to_key,
        )

    def _map_channel_keys_to_connection(self, channel_names, message):
        """
        For a list of channel names, GET

        1. list of their redis keys bucket each one to a dict keyed by the connection index

        2. for each unique channel redis key create a serialized message specific to that redis key, by adding
           the list of channels mapped to that redis key in __asgi_channel__ key to the message

        3. returns a mapping of redis channels keys to their capacity
        """

        # Connection dict keyed by index to list of redis keys mapped on that index
        connection_to_channel_keys = collections.defaultdict(list)
        # Message dict maps redis key to the message that needs to be send on that key
        channel_key_to_message = dict()
        # Channel key mapped to its capacity
        channel_key_to_capacity = dict()

        # For each channel
        for channel in channel_names:
            channel_non_local_name = channel
            if "!" in channel:
                channel_non_local_name = self.non_local_name(channel)
            # Get its redis key
            channel_key = self.prefix + channel_non_local_name
            # Have we come across the same redis key?
            if channel_key not in channel_key_to_message:
                # If not, fill the corresponding dicts
                message = dict(message.items())
                message["__asgi_channel__"] = [channel]
                channel_key_to_message[channel_key] = message
                channel_key_to_capacity[channel_key] = self.get_capacity(channel)
                idx = self.consistent_hash(channel_non_local_name)
                connection_to_channel_keys[idx].append(channel_key)
            else:
                # Yes, Append the channel in message dict
                channel_key_to_message[channel_key]["__asgi_channel__"].append(channel)

        # Now that we know what message needs to be send on a redis key we serialize it
        for key, value in channel_key_to_message.items():
            # Serialize the message stored for each redis key
            channel_key_to_message[key] = self.serialize(value)

        return (
            connection_to_channel_keys,
            channel_key_to_message,
            channel_key_to_capacity,
        )

    def _group_key(self, group):
        """
        Common function to make the storage key for the group.
        """
        return ("%s:group:%s" % (self.prefix, group)).encode("utf8")

    ### Serialization ###

    def serialize(self, message):
        """
        Serializes message to a byte string.
        """
        value = msgpack.packb(message, use_bin_type=True)
        if self.crypter:
            value = self.crypter.encrypt(value)

        # As we use an sorted set to expire messages we need to guarantee uniqueness, with 12 bytes.
        random_prefix = random.getrandbits(8 * 12).to_bytes(12, "big")
        return random_prefix + value

    def deserialize(self, message):
        """
        Deserializes from a byte string.
        """
        # Removes the random prefix
        message = message[12:]

        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)
        return msgpack.unpackb(message, raw=False)

    ### Internal functions ###

    def consistent_hash(self, value):
        """
        Maps the value to a node value between 0 and 4095
        using CRC, then down to one of the ring nodes.
        """
        if isinstance(value, str):
            value = value.encode("utf8")
        bigval = binascii.crc32(value) & 0xFFF
        ring_divisor = 4096 / float(self.ring_size)
        return int(bigval / ring_divisor)

    def make_fernet(self, key):
        """
        Given a single encryption key, returns a Fernet instance using it.
        """
        from cryptography.fernet import Fernet

        if isinstance(key, str):
            key = key.encode("utf8")
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    def __str__(self):
        return "%s(hosts=%s)" % (self.__class__.__name__, self.hosts)

    ### Connection handling ###

    def connection(self, index):
        """
        Returns the correct connection for the index given.
        Lazily instantiates pools.
        """
        # Catch bad indexes
        if not 0 <= index < self.ring_size:
            raise ValueError(
                "There are only %s hosts - you asked for %s!" % (self.ring_size, index)
            )
        # Make a context manager
        return self.ConnectionContextManager(self.pools[index])

    class ConnectionContextManager:
        """
        Async context manager for connections
        """

        def __init__(self, pool):
            self.pool = pool

        async def __aenter__(self):
            self.conn = await self.pool.pop()
            return self.conn

        async def __aexit__(self, exc_type, exc, tb):
            if exc:
                await self.pool.conn_error(self.conn)
            else:
                self.pool.push(self.conn)
            self.conn = None

import asyncio
import base64
import binascii
import collections
import hashlib
import itertools
import random
import string
import time

import aioredis
import msgpack

from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer


class ConnectionPool:
    """
    Connection pool manager for the channel layer.

    It manages a set of connections for the given host specification and
    taking into account asyncio event loops.
    """

    def __init__(self, host):
        self.host = host
        self.conn_map = {}
        self.in_use = {}

    def _ensure_loop(self, loop):
        """
        Get connection list for the specified loop.
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        if loop not in self.conn_map:
            self.conn_map[loop] = []

        return self.conn_map[loop], loop

    async def pop(self, loop=None):
        """
        Get a connection for the given identifier and loop.
        """
        conns, loop = self._ensure_loop(loop)
        if not conns:
            conns.append(await aioredis.create_redis(**self.host, loop=loop))
        conn = conns.pop()
        self.in_use[conn] = loop
        return conn

    def push(self, conn):
        """
        Return a connection to the pool.
        """
        loop = self.in_use[conn]
        del self.in_use[conn]
        conns, _ = self._ensure_loop(loop)
        conns.append(conn)

    def conn_error(self, conn):
        """
        Handle a connection that produced an error.
        """
        conn.close()
        del self.in_use[conn]

    def reset(self):
        """
        Clear all connections from the pool.
        """
        self.conn_map = {}
        self.in_use = {}

    async def close(self):
        """
        Close all connections owned by the pool.
        """
        conn_map = self.conn_map
        in_use = self.in_use
        self.reset()
        for conns in conn_map.values():
            for conn in conns:
                conn.close()
                await conn.wait_closed()
        for conn in in_use:
            conn.close()
            await conn.wait_closed()


class UnsupportedRedis(Exception):
    pass


class RedisChannelLayer(BaseChannelLayer):
    """
    Redis channel layer.

    It routes all messages into remote Redis server. Support for
    sharding among different Redis installations and message
    encryption are provided.
    """

    blpop_timeout = 5
    queue_get_timeout = 10

    def __init__(
        self,
        hosts=None,
        prefix="asgi:",
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
        # TODO: ensure uniqueness better, e.g. Redis keys with SETNX
        self.client_prefix = "".join(random.choice(string.ascii_letters) for i in range(8))
        # Set up any encryption objects
        self._setup_encryption(symmetric_encryption_keys)
        # Number of coroutines trying to receive right now
        self.receive_count = 0
        # Event loop they are trying to receive on
        self.receive_event_loop = None
        # Main receive loop running
        self.receive_loop_task = None
        # Buffered messages by process-local channel name
        self.receive_buffer = collections.defaultdict(asyncio.Queue)

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
            raise ValueError("You must pass a list of Redis hosts, even if there is only one.")
        # Decode each hosts entry into a kwargs dict
        result = []
        for entry in hosts:
            if isinstance(entry, dict):
                result.append(entry)
            else:
                result.append({
                    "address": entry,
                })
        return result

    def _setup_encryption(self, symmetric_encryption_keys):
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, (str, bytes)):
                raise ValueError("symmetric_encryption_keys must be a list of possible keys")
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError("Cannot run with encryption without 'cryptography' installed.")
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
            # Check the length of the list before send
            # This can allow the list to leak slightly over capacity, but that's fine.
            if await connection.llen(channel_key) >= self.get_capacity(channel):
                raise ChannelFull()
            # Push onto the list then set it to expire in case it's not consumed
            await connection.rpush(channel_key, self.serialize(message))
            await connection.expire(channel_key, int(self.expiry))

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
            assert real_channel.endswith(self.client_prefix + "!"), "Wrong client prefix"
            # Enter receiving section
            loop = asyncio.get_event_loop()
            self.receive_count += 1
            try:
                if self.receive_count == 1:
                    # If we're the first coroutine in, make a receive loop!
                    general_channel = self.non_local_name(channel)
                    self.receive_loop_task = loop.create_task(self.receive_loop(general_channel))
                    self.receive_event_loop = loop
                else:
                    # Otherwise, check our event loop matches
                    if self.receive_event_loop != loop:
                        raise RuntimeError("Two event loops are trying to receive() on one channel layer at once!")
                    if self.receive_loop_task.done():
                        # Maybe raise an exception from the task
                        self.receive_loop_task.result()
                        # Raise our own exception if that failed
                        raise RuntimeError("Redis receive loop exited early")

                # Wait for our message to appear
                while True:
                    try:
                        message = await asyncio.wait_for(self.receive_buffer[channel].get(), self.queue_get_timeout)
                        if self.receive_buffer[channel].empty():
                            del self.receive_buffer[channel]
                        return message
                    except asyncio.TimeoutError:
                        # See if we need to propagate a dead receiver exception
                        if self.receive_loop_task.done():
                            self.receive_loop_task.result()

            finally:
                self.receive_count -= 1
                # If we were the last out, stop the receive loop
                if self.receive_count == 0:
                    self.receive_loop_task.cancel()
        else:
            # Do a plain direct receive
            return (await self.receive_single(channel))[1]

    async def receive_loop(self, general_channel):
        """
        Continuous-receiving loop that makes sure something is fetching results
        for the channel passed in.
        """
        assert general_channel.endswith("!"), "receive_loop not called on general queue of process-local channel"
        while True:
            real_channel, message = await self.receive_single(general_channel)
            if type(real_channel) is list:
                for channel in real_channel:
                    await self.receive_buffer[channel].put(message)
            else:
                await self.receive_buffer[real_channel].put(message)

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
        # Get that connection and receive off of it
        async with self.connection(index) as connection:
            channel_key = self.prefix + channel
            content = None

            while content is None:
                content = await connection.blpop(channel_key, timeout=self.blpop_timeout)
            # Message decode
            message = self.deserialize(content[1])
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
        # TODO: Guarantee uniqueness better?
        return "%s.%s!%s" % (
            prefix,
            self.client_prefix,
            "".join(random.choice(string.ascii_letters) for i in range(12)),
        )

    ### Flush extension ###

    async def flush(self):
        """
        Deletes all messages and groups on all shards.
        """
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
                await connection.eval(
                    delete_prefix,
                    keys=[],
                    args=[self.prefix + "*"]
                )
        # Now clear the pools as well
        await self.close_pools()

    async def close_pools(self):
        """
        Close all connections in the event loop pools.
        """
        for pool in self.pools:
            await pool.close()

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
            await connection.zadd(
                group_key,
                time.time(),
                channel,
            )
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
            await connection.zrem(
                key,
                channel,
            )

    async def group_send(self, group, message):
        """
        Sends a message to the entire group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        # Retrieve list of all channel names
        key = self._group_key(group)
        async with self.connection(self.consistent_hash(group)) as connection:
            # Discard old channels based on group_expiry
            await connection.zremrangebyscore(key, min=0, max=int(time.time()) - self.group_expiry)

            channel_names = [x.decode("utf8") for x in await connection.zrange(key, 0, -1)]

        connection_to_channel_keys, channel_keys_to_message, channel_keys_to_capacity = \
            self._map_channel_keys_to_connection(channel_names, message)

        for connection_index, channel_redis_keys in connection_to_channel_keys.items():

            # Create a LUA script specific for this connection.
            # Make sure to use the message specific to this channel, it is
            # stored in channel_to_message dict and contains the
            # __asgi_channel__ key.

            group_send_lua = """
                    for i=1,#KEYS do
                        if redis.call('LLEN', KEYS[i]) < tonumber(ARGV[i + #KEYS]) then
                            redis.call('RPUSH', KEYS[i], ARGV[i])
                            redis.call('EXPIRE', KEYS[i], %d)
                        end
                    end
                    """ % self.expiry

            # We need to filter the messages to keep those related to the connection
            args = [channel_keys_to_message[channel_key] for channel_key in channel_redis_keys]

            # We need to send the capacity for each channel
            args += [channel_keys_to_capacity[channel_key] for channel_key in channel_redis_keys]

            # channel_keys does not contain a single redis key more than once
            async with self.connection(connection_index) as connection:
                await connection.eval(group_send_lua, keys=channel_redis_keys, args=args)

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

        return connection_to_channels, channel_to_message, channel_to_capacity, channel_to_key

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
            if channel_key not in channel_key_to_message.keys():
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
        for key in channel_key_to_message.keys():
            # Serialize the message stored for each redis key
            channel_key_to_message[key] = self.serialize(channel_key_to_message[key])

        return connection_to_channel_keys, channel_key_to_message, channel_key_to_capacity

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
        return value

    def deserialize(self, message):
        """
        Deserializes from a byte string.
        """
        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)
        return msgpack.unpackb(message, encoding="utf8")

    ### Internal functions ###

    def consistent_hash(self, value):
        """
        Maps the value to a node value between 0 and 4095
        using CRC, then down to one of the ring nodes.
        """
        if isinstance(value, str):
            value = value.encode("utf8")
        bigval = binascii.crc32(value) & 0xfff
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
            raise ValueError("There are only %s hosts - you asked for %s!" % (self.ring_size, index))
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
                self.pool.conn_error(self.conn)
            else:
                self.pool.push(self.conn)
            self.conn = None

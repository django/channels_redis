from __future__ import unicode_literals

import asyncio
import base64
import binascii
import hashlib
import itertools
import random
import string

import aioredis
import msgpack

from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer


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
    local_poll_interval = 0.01

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
        self.channel_capacity = channel_capacity or {}
        self.prefix = prefix
        self.pools = {}
        assert isinstance(self.prefix, str), "Prefix must be unicode"
        # Configure the host objects
        self.hosts = self.decode_hosts(hosts)
        self.ring_size = len(self.hosts)
        # Normal channels choose a host index by cycling through the available hosts
        self._receive_index_generator = itertools.cycle(range(len(self.hosts)))
        self._send_index_generator = itertools.cycle(range(len(self.hosts)))
        # Decide on a unique client prefix to use in ! sections
        # TODO: ensure uniqueness better, e.g. Redis keys with SETNX
        self.client_prefix = "".join(random.choice(string.ascii_letters) for i in range(8))
        # Set up any encryption objects
        self._setup_encryption(symmetric_encryption_keys)
        # Buffered messages by process-local channel name
        self.receive_buffer = {}
        # Coroutines currently receiving the process-local channel.
        self.receive_tasks = {}

    def decode_hosts(self, hosts):
        """
        Takes the value of the "hosts" argument passed to the class and returns
        a list of kwargs to use for the Redis connection constructor.
        """
        # If no hosts were provided, return a default value
        if not hosts:
            return {"address": ("localhost", 6379)}
        # If they provided just a string, scold them.
        if isinstance(hosts, (str, bytes)):
            raise ValueError("You must pass a list of Redis hosts, even if there is only one.")
        # Decode each hosts entry into a kwargs dict
        result = []
        for entry in hosts:
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
        if "!" in channel:
            message = dict(message.items())
            message["__asgi_channel__"] = channel
            channel = self.non_local_name(channel)
        # Write out message into expiring key (avoids big items in list)
        channel_key = self.prefix + channel
        # Pick a connection to the right server - consistent for specific
        # channels, random for general channels
        if "!" in channel:
            index = self.consistent_hash(channel)
            pool = await self.connection(index)
        else:
            index = next(self._send_index_generator)
            pool = await self.connection(index)
        with (await pool) as connection:
            # Check the length of the list before send
            # This can allow the list to leak slightly over capacity, but that's fine.
            if await connection.llen(channel_key) > self.get_capacity(channel):
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
            # Make sure a receive task is running
            task = self.receive_tasks.get(real_channel, None)
            if task is not None and task.done():
                task = None
            if task is None:
                self.receive_tasks[real_channel] = asyncio.ensure_future(
                    self.receive_loop(real_channel),
                )
            # Wait on the receive buffer's contents
            return await self.receive_buffer_lpop(channel)
        else:
            # Do a plain direct receive
            return (await self.receive_single(channel))[1]

    async def receive_loop(self, channel):
        """
        Continuous-receiving loop that fetches results into the receive buffer.
        """
        assert "!" in channel, "receive_loop called on non-process-local channel"
        while True:
            # Catch RuntimeErrors from the loop stopping while we release
            # a connection. Wish there was a cleaner solution here.
            real_channel, message = await self.receive_single(channel)
            self.receive_buffer.setdefault(real_channel, []).append(message)

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
        pool = await self.connection(index)
        with (await pool) as connection:
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

    async def receive_buffer_lpop(self, channel):
        """
        Atomic, async method that returns the left-hand item in a receive buffer.
        """
        # TODO: Use locks or something, not a poll
        while True:
            if self.receive_buffer.get(channel, None):
                message = self.receive_buffer[channel][0]
                if len(self.receive_buffer[channel]) == 1:
                    del self.receive_buffer[channel]
                else:
                    self.receive_buffer[channel] = self.receive_buffer[channel][1:]
                return message
            else:
                # See if we need to propagate a dead receiver exception
                real_channel = self.non_local_name(channel)
                task = self.receive_tasks.get(real_channel, None)
                if task is not None and task.done():
                    task.result()
                # Sleep poll
                await asyncio.sleep(self.local_poll_interval)

    def new_channel(self, prefix="specific."):
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
            connection = await self.connection(i)
            await connection.eval(
                delete_prefix,
                keys=[],
                args=[self.prefix + "*"]
            )

    async def close(self):
        # Stop all reader tasks
        for task in self.receive_tasks.values():
            task.cancel()
        asyncio.wait(self.receive_tasks.values())
        self.receive_tasks = {}
        # Close up all pools
        for pool in self.pools.values():
            pool.close()
            await pool.wait_closed()

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

    async def connection(self, index):
        """
        Returns the correct connection for the index given.
        Lazily instantiates pools.
        """
        # Catch bad indexes
        if not 0 <= index < self.ring_size:
            raise ValueError("There are only %s hosts - you asked for %s!" % (self.ring_size, index))
        # Make a pool if needed and return it
        if index not in self.pools:
            self.pools[index] = await aioredis.create_redis_pool(**self.hosts[index])
        return self.pools[index]

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

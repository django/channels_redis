from __future__ import unicode_literals

import base64
import binascii
import hashlib
import math
import msgpack
import pkg_resources
import random
import redis
import six
import string
import time
import uuid

from asgiref.base_layer import BaseChannelLayer


__version__ = pkg_resources.require('asgi_redis')[0].version


class RedisChannelLayer(BaseChannelLayer):
    """
    ORM-backed channel environment. For development use only; it will span
    multiple processes fine, but it's going to be pretty bad at throughput.
    """

    blpop_timeout = 5

    def __init__(self, expiry=60, hosts=None, prefix="asgi:", group_expiry=86400, capacity=100, channel_capacity=None,
                 symmetric_encryption_keys=None):
        super(RedisChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
        # Make sure they provided some hosts, or provide a default
        if not hosts:
            hosts = [("localhost", 6379)]
        self.hosts = []
        for entry in hosts:
            if isinstance(entry, six.string_types):
                self.hosts.append(entry)
            else:
                self.hosts.append("redis://%s:%d/0" % (entry[0],entry[1]))
        self.prefix = prefix
        assert isinstance(self.prefix, six.text_type), "Prefix must be unicode"
        # Precalculate some values for ring selection
        self.ring_size = len(self.hosts)
        self.ring_divisor = int(math.ceil(4096 / float(self.ring_size)))
        # Create connections ahead of time (they won't call out just yet, but
        # we want to connection-pool them later)
        self._connection_list = [
            redis.Redis.from_url(host)
            for host in self.hosts
        ]
        # Decide on a unique client prefix to use in ! sections
        # TODO: ensure uniqueness better, e.g. Redis keys with SETNX
        self.client_prefix = "".join(random.choice(string.ascii_letters) for i in range(8))
        # Register scripts
        connection = self.connection(None)
        self.chansend = connection.register_script(self.lua_chansend)
        self.lpopmany = connection.register_script(self.lua_lpopmany)
        self.delprefix = connection.register_script(self.lua_delprefix)
        # See if we can do encryption if they asked
        if symmetric_encryption_keys:
            if isinstance(symmetric_encryption_keys, six.string_types):
                raise ValueError("symmetric_encryption_keys must be a list of possible keys")
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError("Cannot run with encryption without 'cryptography' installed.")
            sub_fernets = [self.make_fernet(key) for key in symmetric_encryption_keys]
            self.crypter = MultiFernet(sub_fernets)
        else:
            self.crypter = None

    ### ASGI API ###

    extensions = ["groups", "flush"]

    def send(self, channel, message):
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert isinstance(channel, six.text_type), "%s is not unicode" % channel
        # Write out message into expiring key (avoids big items in list)
        # TODO: Use extended set, drop support for older redis?
        message_key = self.prefix + uuid.uuid4().hex
        channel_key = self.prefix + channel
        # Pick a connection to the right server - consistent for response
        # channels, random for normal channels
        if "!" in channel:
            index = self.consistent_hash(channel)
            connection = self.connection(index)
        else:
            connection = self.connection(None)
        # Use the Lua function to do the set-and-push
        try:
            self.chansend(
                keys=[message_key, channel_key],
                args=[self.serialize(message), self.expiry, self.get_capacity(channel)],
            )
        except redis.exceptions.ResponseError as e:
            # The Lua script handles capacity checking and sends the "full" error back
            if e.args[0] == "full":
                raise self.ChannelFull

    def receive_many(self, channels, block=False):
        if not channels:
            return None, None
        channels = list(channels)
        assert all(isinstance(channel, six.text_type) for channel in channels)
        # Work out what servers to listen on for the given channels
        indexes = {}
        random_index = self.random_index()
        for channel in channels:
            if "!" in channel:
                indexes.setdefault(self.consistent_hash(channel), []).append(channel)
            else:
                indexes.setdefault(random_index, []).append(channel)
        # Get a message from one of our channels
        while True:
            # Select a random connection to use
            index = random.choice(list(indexes.keys()))
            connection = self.connection(index)
            channels = indexes[index]
            # Shuffle channels to avoid the first ones starving others of workers
            random.shuffle(channels)
            # Pop off any waiting message
            list_names = [self.prefix + channel for channel in channels]
            if block:
                result = connection.blpop(list_names, timeout=self.blpop_timeout)
            else:
                result = self.lpopmany(keys=list_names, client=connection)
            if result:
                content = connection.get(result[1])
                # If the content key expired, keep going.
                if content is None:
                    continue
                # Return the channel it's from and the message
                return result[0][len(self.prefix):].decode("utf8"), self.deserialize(content)
            else:
                return None, None

    def new_channel(self, pattern):
        assert isinstance(pattern, six.text_type)
        # Keep making channel names till one isn't present.
        while True:
            random_string = "".join(random.choice(string.ascii_letters) for i in range(12))
            assert pattern.endswith("!")
            new_name = pattern + random_string
            # Get right connection
            index = self.consistent_hash(new_name)
            connection = self.connection(index)
            # Check to see if it's in the connected Redis.
            # This fails to stop collisions for sharding where the channel is
            # non-single-listener, but that seems very unlikely.
            key = self.prefix + new_name
            if not connection.exists(key):
                return new_name

    ### ASGI Group extension ###

    def group_add(self, group, channel):
        """
        Adds the channel to the named group for at least 'expiry'
        seconds (expiry defaults to message expiry if not provided).
        """
        group_key = self._group_key(group)
        connection = self.connection(self.consistent_hash(group))
        # Add to group sorted set with creation time as timestamp
        connection.zadd(
            group_key,
            **{channel: time.time()}
        )
        # Set both expiration to be group_expiry, since everything in
        # it at this point is guaranteed to expire before that
        connection.expire(group_key, self.group_expiry)
        # Also add to a normal set that contains all the groups a channel is in
        # (as yet unused)
        channel_key = self._channel_groups_key(channel)
        connection = self.connection(self.consistent_hash(channel))
        connection.sadd(channel_key, group)
        connection.expire(channel_key, self.group_expiry)

    def group_discard(self, group, channel):
        """
        Removes the channel from the named group if it is in the group;
        does nothing otherwise (does not error)
        """
        key = self._group_key(group)
        self.connection(self.consistent_hash(group)).zrem(
            key,
            channel,
        )

    def send_group(self, group, message):
        """
        Sends a message to the entire group.
        """
        # TODO: More efficient implementation (lua script per shard?)
        for channel in self._group_channels(group):
            try:
                self.send(channel, message)
            except self.ChannelFull:
                pass

    def _group_key(self, group):
        return ("%s:group:%s" % (self.prefix, group)).encode("utf8")

    def _channel_groups_key(self, group):
        return ("%s:chgroups:%s" % (self.prefix, group)).encode("utf8")

    def _group_channels(self, group):
        """
        Returns an iterable of all channels in the group.
        """
        key = self._group_key(group)
        connection = self.connection(self.consistent_hash(group))
        # Discard old channels based on group_expiry
        connection.zremrangebyscore(key, 0, int(time.time()) - self.group_expiry)
        # Return current lot
        return [x.decode("utf8") for x in connection.zrange(
            key,
            0,
            -1,
        )]

    ### Flush extension ###

    def flush(self):
        """
        Deletes all messages and groups on all shards.
        """
        for connection in self._connection_list:
            self.delprefix(keys=[], args=[self.prefix+"*"], client=connection)

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

    ### Redis Lua scripts ###

    # Single-command channel send. Returns error if over capacity.
    # Keys: message, channel_list
    # Args: content, expiry, capacity
    lua_chansend = """
        if redis.call('llen', KEYS[2]) >= tonumber(ARGV[3]) then
            return redis.error_reply("full")
        end
        redis.call('set', KEYS[1], ARGV[1])
        redis.call('expire', KEYS[1], ARGV[2])
        redis.call('rpush', KEYS[2], KEYS[1])
        redis.call('expire', KEYS[2], ARGV[2] + 1)
    """

    lua_lpopmany = """
        for keyCount = 1, #KEYS do
            local result = redis.call('LPOP', KEYS[keyCount])
            if result then
                return {KEYS[keyCount], result}
            end
        end
        return {nil, nil}
    """

    lua_delprefix = """
        local keys = redis.call('keys', ARGV[1])
        for i=1,#keys,5000 do
            redis.call('del', unpack(keys, i, math.min(i+4999, #keys)))
        end
    """

    ### Internal functions ###

    def consistent_hash(self, value):
        """
        Maps the value to a node value between 0 and 4095
        using MD5, then down to one of the ring nodes.
        """
        if isinstance(value, six.text_type):
            value = value.encode("utf8")
        bigval = binascii.crc32(value) & 0xffffffff
        return (bigval // 0x100000) // self.ring_divisor

    def random_index(self):
        return random.randint(0, len(self.hosts) - 1)

    def connection(self, index):
        """
        Returns the correct connection for the current thread.

        Pass key to use a server based on consistent hashing of the key value;
        pass None to use a random server instead.
        """
        # If index is explicitly None, pick a random server
        if index is None:
            index = self.random_index()
        # Catch bad indexes
        if not 0 <= index < self.ring_size:
            raise ValueError("There are only %s hosts - you asked for %s!" % (self.ring_size, index))
        return self._connection_list[index]

    def make_fernet(self, key):
        """
        Given a single encryption key, returns a Fernet instance using it.
        """
        from cryptography.fernet import Fernet
        if isinstance(key, six.text_type):
            key = key.encode("utf8")
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    def __str__(self):
        return "%s(hosts=%s)" % (self.__class__.__name__, self.hosts)


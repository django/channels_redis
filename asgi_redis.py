from __future__ import unicode_literals
import pkg_resources
import binascii
import math
import msgpack
import random
import redis
import six
import string
import time
import uuid


__version__ = pkg_resources.require('asgi_redis')[0].version


class RedisChannelLayer(object):
    """
    ORM-backed channel environment. For development use only; it will span
    multiple processes fine, but it's going to be pretty bad at throughput.
    """

    blpop_timeout = 5

    def __init__(self, expiry=60, hosts=None, prefix="asgi:"):
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
        self.expiry = expiry
        # Precalculate some values for ring selection
        self.ring_size = len(self.hosts)
        self.ring_divisor = int(math.ceil(4096 / float(self.ring_size)))

    ### ASGI API ###

    extensions = ["groups"]

    class MessageTooLarge(Exception):
        pass

    def send(self, channel, message):
        # Typecheck
        assert isinstance(message, dict), "message is not a dict"
        assert isinstance(channel, six.text_type), "%s is not unicode" % channel
        # Write out message into expiring key (avoids big items in list)
        # TODO: Use extended set, drop support for older redis?
        key = self.prefix + uuid.uuid4().hex
        # Pick a connection to the right server - consistent for response
        # channels, random for normal channels
        if channel.startswith("!"):
            index = self.consistent_hash(channel)
            connection = self.connection(index)
        else:
            connection = self.connection(None)
        # Make a key for the message and set to expire
        connection.set(
            key,
            self.serialize(message),
        )
        connection.expire(
            key,
            self.expiry,
        )
        # Add key to list
        connection.rpush(
            self.prefix + channel,
            key,
        )
        # Set list to expire when message does (any later messages will bump this)
        connection.expire(
            self.prefix + channel,
            self.expiry + 10,
        )

    def receive_many(self, channels, block=False):
        if not channels:
            return None, None
        channels = list(channels)
        assert all(isinstance(channel, six.text_type) for channel in channels)
        # Work out what servers to listen on for the given channels
        indexes = {}
        random_index = self.random_index()
        for channel in channels:
            if channel.startswith("!"):
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
                # TODO: More efficient non-blocking popping scheme.
                for list_name in list_names:
                    result = connection.lpop(list_name)
                    if result:
                        result = [list_name, result]
                        break
            if result:
                content = connection.get(result[1])
                # If the content key expired, keep going.
                if content is None:
                    continue
                # Return the channel it's from and the message
                return result[0][len(self.prefix):], self.deserialize(content)
            else:
                return None, None

    def new_channel(self, pattern):
        assert isinstance(pattern, six.text_type)
        # Keep making channel names till one isn't present.
        while True:
            random_string = "".join(random.choice(string.ascii_letters) for i in range(8))
            new_name = pattern.replace("?", random_string)
            # Get right connection; most uses of this should end up with !
            if new_name.startswith("!"):
                index = self.consistent_hash(new_name)
                connection = self.connection(index)
            else:
                connection = self.connection(None)
            # Check to see if it's in the connected Redis.
            # This fails to stop collisions for sharding where the channel is
            # non-single-listener, but that seems very unlikely.
            key = self.prefix + new_name
            if not connection.exists(key):
                return new_name

    ### ASGI Group extension ###

    def group_add(self, group, channel, expiry=None):
        """
        Adds the channel to the named group for at least 'expiry'
        seconds (expiry defaults to message expiry if not provided).
        """
        key = "%s:group:%s" % (self.prefix, group)
        key = key.encode("utf8")
        self.connection(self.consistent_hash(group)).zadd(
            key,
            **{channel: time.time() + (expiry or self.expiry)}
        )

    def group_discard(self, group, channel):
        """
        Removes the channel from the named group if it is in the group;
        does nothing otherwise (does not error)
        """
        key = "%s:group:%s" % (self.prefix, group)
        key = key.encode("utf8")
        self.connection(self.consistent_hash(group)).zrem(
            key,
            channel,
        )

    def send_group(self, group, message):
        """
        Sends a message to the entire group.
        """
        for channel in self._group_channels(group):
            self.send(channel, message)

    def _group_channels(self, group):
        """
        Returns an iterable of all channels in the group.
        """
        key = "%s:group:%s" % (self.prefix, group)
        key = key.encode("utf8")
        connection = self.connection(self.consistent_hash(group))
        # Discard old channels
        connection.zremrangebyscore(key, 0, int(time.time()) - 10)
        # Return current lot
        return [x.decode("utf8") for x in connection.zrange(
            key,
            0,
            -1,
        )]

    ### Serialization ###

    def serialize(self, message):
        """
        Serializes message to a byte string.
        """
        return msgpack.packb(message, use_bin_type=True)

    def deserialize(self, message):
        """
        Deserializes from a byte string.
        """
        return msgpack.unpackb(message, encoding="utf8")

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
        return redis.Redis.from_url(self.hosts[index])

    def __str__(self):
        return "%s(hosts=%s)" % (self.__class__.__name__, self.hosts)


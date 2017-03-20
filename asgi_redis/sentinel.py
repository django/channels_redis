from __future__ import unicode_literals
from asgi_redis import RedisChannelLayer
from redis import sentinel
import random
import six
import string
random.seed()


class RedisSentinelChannelLayer(RedisChannelLayer):
    """
    Variant of the Redis channel layer that supports the Redis Sentinel HA protocol.

    Supports sharding, but assumes that there is only one sentinel cluster with multiple redis services
    monitored by that cluster. So, this will only connect to a single cluster of Sentinel servers,
    but will suppport sharding by asking that sentinel cluster for different services. Also, any redis connection
    options (socket timeout, socket keepalive, etc) will be assumed to be identical across redis server, and
    across all services.

    "hosts" in this arrangement, is used to list the redis sentinel hosts. As such, it only supports the
    tuple method of specifying hosts, as that's all the redis sentinel python library supports at the moment.

    "services" is the list of redis services monitored by the sentinel system that redis keys will be distributed
     across.
    """
    def __init__(
                self,
                expiry=60,
                hosts=None,
                prefix="asgi:",
                group_expiry=86400,
                capacity=100,
                channel_capacity=None,
                symmetric_encryption_keys=None,
                stats_prefix="asgi-meta:",
                socket_connect_timeout=None,
                socket_timeout=None,
                socket_keepalive=None,
                socket_keepalive_options=None,
                services=None,
        ):
            super(RedisChannelLayer, self).__init__(
                expiry=expiry,
                group_expiry=group_expiry,
                capacity=capacity,
                channel_capacity=channel_capacity,
            )
            # Make sure they provided some hosts, or provide a default
            if not hosts:
                hosts = [("localhost", 26379)]
            self.hosts = list()
            self.services = list()

            if not services:
                raise ValueError("Must specify at least one service name monitored by Sentinel")

            if isinstance(services, six.string_types):
                raise ValueError("Sentinel service types must be specified as an iterable list of strings")

            for entry in services:
                if not isinstance(entry, six.string_types):
                    raise ValueError("Sentinel service types must be specified as strings.")
                else:
                    self.services.append(entry)

            if isinstance(hosts, six.string_types):
                # user accidentally used one host string instead of providing a list of hosts
                raise ValueError('ASGI Redis hosts must be specified as an iterable list of hosts.')

            for entry in hosts:
                if isinstance(entry, six.string_types):
                    raise ValueError("Sentinel Redis host entries must be specified as tuples, not strings.")
                else:
                    self.hosts.append(entry)
            self.prefix = prefix
            assert isinstance(self.prefix, six.text_type), "Prefix must be unicode"
            # Precalculate some values for ring selection
            self.ring_size = len(self.services)
            # Create connections ahead of time (they won't call out just yet, but
            # we want to connection-pool them later)
            if socket_timeout and socket_timeout < self.blpop_timeout:
                raise ValueError("The socket timeout must be at least %s seconds" % self.blpop_timeout)
            self._sentinel = self._generate_sentinel(
                redis_kwargs={
                    "socket_connect_timeout": socket_connect_timeout,
                    "socket_timeout": socket_timeout,
                    "socket_keepalive": socket_keepalive,
                    "socket_keepalive_options": socket_keepalive_options,
                },
            )
            # Decide on a unique client prefix to use in ! sections
            # TODO: ensure uniqueness better, e.g. Redis keys with SETNX
            self.client_prefix = "".join(random.choice(string.ascii_letters) for i in range(8))
            # Register scripts
            connection = self.connection(None)
            self.chansend = connection.register_script(self.lua_chansend)
            self.lpopmany = connection.register_script(self.lua_lpopmany)
            self.delprefix = connection.register_script(self.lua_delprefix)
            self.incrstatcounters = connection.register_script(self.lua_incrstatcounters)
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
            self.stats_prefix = stats_prefix

    def _generate_sentinel(self, redis_kwargs):
        # pass redis_kwargs through to the sentinel object to be used for each connection to the redis servers.
        return sentinel.Sentinel(self.hosts, **redis_kwargs)

    def flush(self):
        """
        Deletes all messages and groups on all shards.
        """
        for service_name in self.services:
            connection = self._sentinel.master_for(service_name)
            self.delprefix(keys=[], args=[self.prefix + "*"], client=connection)
            self.delprefix(keys=[], args=[self.stats_prefix + "*"], client=connection)

    def connection(self, index):
        # return the master for the given index
        # If index is explicitly None, pick a random server
        if index is None:
            index = self.random_index()
        # Catch bad indexes
        if not 0 <= index < self.ring_size:
            raise ValueError("There are only %s hosts - you asked for %s!" % (self.ring_size, index))
        service_name = self.services[index]
        return self._sentinel.master_for(service_name)

    def random_index(self):
        return random.randint(0, len(self.services) - 1)

    def global_statistics(self):
        """
        Returns dictionary of statistics across all channels on all shards.
        Return value is a dictionary with following fields:
            * messages_count, the number of messages processed since server start
            * channel_full_count, the number of times ChannelFull exception has been risen since server start

        This implementation does not provide calculated per second values.
        Due perfomance concerns, does not provide aggregated messages_pending and messages_max_age,
        these are only avaliable per channel.

        """
        statistics = {
            self.STAT_MESSAGES_COUNT: 0,
            self.STAT_CHANNEL_FULL: 0,
        }
        prefix = self.stats_prefix + self.global_stats_key
        for service_name in self.services:
            connection = self._sentinel.master_for(service_name)
            messages_count, channel_full_count = connection.mget(
                ':'.join((prefix, self.STAT_MESSAGES_COUNT)),
                ':'.join((prefix, self.STAT_CHANNEL_FULL)),
            )
            statistics[self.STAT_MESSAGES_COUNT] += int(messages_count or 0)
            statistics[self.STAT_CHANNEL_FULL] += int(channel_full_count or 0)

        return statistics

    def channel_statistics(self, channel):
        """
        Returns dictionary of statistics for specified channel.
        Return value is a dictionary with following fields:
            * messages_count, the number of messages processed since server start
            * messages_pending, the current number of messages waiting
            * messages_max_age, how long the oldest message has been waiting, in seconds
            * channel_full_count, the number of times ChannelFull exception has been risen since server start

        This implementation does not provide calculated per second values
        """
        statistics = {
            self.STAT_MESSAGES_COUNT: 0,
            self.STAT_MESSAGES_PENDING: 0,
            self.STAT_MESSAGES_MAX_AGE: 0,
            self.STAT_CHANNEL_FULL: 0,
        }
        prefix = self.stats_prefix + channel

        if "!" in channel or "?" in channel:
            connections = [self.connection(self.consistent_hash(channel))]
        else:
            # if we don't know where it is, we have to check in all shards
            connections = [self._sentinel.master_for(service_name) for service_name in self.services]

        channel_key = self.prefix + channel

        for connection in connections:
            messages_count, channel_full_count = connection.mget(
                ':'.join((prefix, self.STAT_MESSAGES_COUNT)),
                ':'.join((prefix, self.STAT_CHANNEL_FULL)),
            )
            statistics[self.STAT_MESSAGES_COUNT] += int(messages_count or 0)
            statistics[self.STAT_CHANNEL_FULL] += int(channel_full_count or 0)
            statistics[self.STAT_MESSAGES_PENDING] += connection.llen(channel_key)
            oldest_message = connection.lindex(channel_key, 0)
            if oldest_message:
                messages_age = self.expiry - connection.ttl(oldest_message)
                statistics[self.STAT_MESSAGES_MAX_AGE] = max(statistics[self.STAT_MESSAGES_MAX_AGE], messages_age)
        return statistics

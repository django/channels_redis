from __future__ import unicode_literals
from asgi_redis import RedisChannelLayer
from redis import sentinel
import random
import six
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
                services=None):
            self.services = self._setup_services(services)

            # Precalculate some values for ring selection
            self.ring_size = len(self.services)

            self.hosts = self._setup_hosts(hosts)

            self._sentinel = self._generate_sentinel(
                redis_kwargs={
                    "socket_connect_timeout": socket_connect_timeout,
                    "socket_timeout": socket_timeout,
                    "socket_keepalive": socket_keepalive,
                    "socket_keepalive_options": socket_keepalive_options,
                },
            )

            super(RedisSentinelChannelLayer, self).__init__(expiry,
                                                            hosts,
                                                            prefix,
                                                            group_expiry,
                                                            capacity,
                                                            channel_capacity,
                                                            symmetric_encryption_keys,
                                                            stats_prefix,
                                                            socket_connect_timeout,
                                                            socket_timeout,
                                                            socket_keepalive,
                                                            socket_keepalive_options)

    def _setup_services(self, services):
        final_services = list()

        if not services:
            raise ValueError("Must specify at least one service name monitored by Sentinel")

        if isinstance(services, six.string_types):
            raise ValueError("Sentinel service types must be specified as an iterable list of strings")

        for entry in services:
            if not isinstance(entry, six.string_types):
                raise ValueError("Sentinel service types must be specified as strings.")
            else:
                final_services.append(entry)
        return final_services

    def _setup_hosts(self, hosts):
        if not hosts:
            hosts = [("localhost", 26379)]
        final_hosts = list()
        if isinstance(hosts, six.string_types):
            # user accidentally used one host string instead of providing a list of hosts
            raise ValueError('ASGI Redis hosts must be specified as an iterable list of hosts.')

        for entry in hosts:
            if isinstance(entry, six.string_types):
                raise ValueError("Sentinel Redis host entries must be specified as tuples, not strings.")
            else:
                final_hosts.append(entry)
        return final_hosts

    def _generate_sentinel(self, redis_kwargs):
        # pass redis_kwargs through to the sentinel object to be used for each connection to the redis servers.
        return sentinel.Sentinel(self.hosts, **redis_kwargs)

    def _generate_connections(self, redis_kwargs):
        # override this for purposes of super's init.
        return list()

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

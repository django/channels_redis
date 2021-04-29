import asyncio
import sys
import traceback
import uuid

import aioredis
import msgpack


class RedisPubSubChannelLayer:
    """
    Channel Layer that uses Redis's pub/sub functionality.
    """

    def __init__(self, hosts, prefix="asgi", **kwargs):
        assert (
            isinstance(hosts, list) and len(hosts) > 0
        ), "`hosts` must be a list with at least one Redis server"

        self.prefix = prefix

        # Each consumer gets its own *specific* channel, created with the `new_channel()` method.
        # This dict maps `channel_name` to a queue of messages for that channel.
        self.channels = {}

        # A channel can subscribe to zero or more groups.
        # This dict maps `group_name` to set of channel names who are subscribed to that group.
        self.groups = {}

        # For each host, we create a `RedisSingleShardConnection` to manage the connection to that host.
        self._shards = [RedisSingleShardConnection(host, self) for host in hosts]

    def _get_shard(self, channel_or_group_name):
        """
        Return the shard that is used exclusively for this channel or group.
        """
        if len(self._shards) == 1:
            # Avoid the overhead of hashing and modulo when it is unnecessary.
            return self._shards[0]
        shard_index = abs(hash(channel_or_group_name)) % len(self._shards)
        return self._shards[shard_index]

    def _get_group_channel_name(self, group):
        """
        Return the channel name used by a group.
        Includes '__group__' in the returned
        string so that these names are distinguished
        from those returned by `new_channel()`.
        Technically collisions are possible, but it
        takes what I believe is intentional abuse in
        order to have colliding names.
        """
        return f"{self.prefix}__group__{group}"

    extensions = ["groups"]

    ################################################################################
    # Channel layer API
    ################################################################################

    async def send(self, channel, message):
        """
        Send a message onto a (general or specific) channel.
        """
        shard = self._get_shard(channel)
        await shard.publish(channel, message)

    async def new_channel(self, prefix="specific."):
        """
        Returns a new channel name that can be used by a consumer in our
        process as a specific channel.
        """
        channel = f"{self.prefix}{prefix}{uuid.uuid4().hex}"
        self.channels[channel] = asyncio.Queue()
        shard = self._get_shard(channel)
        await shard.subscribe(channel)
        return channel

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.
        If more than one coroutine waits on the same channel, a random one
        of the waiting coroutines will get the result.
        """
        if channel not in self.channels:
            raise RuntimeError(
                'You should only call receive() on channels that you "own" and that were created with `new_channel()`.'
            )

        q = self.channels[channel]

        try:
            message = await q.get()
        except asyncio.CancelledError:
            # We assume here that the reason we are cancelled is because the consumer
            # is exiting, therefore we need to cleanup by unsubscribe below. Indeed,
            # currently the way that Django Channels works, this is a safe assumption.
            # In the future, Dajngo Channels could change to call a *new* method that
            # would serve as the antithesis of `new_channel()`; this new method might
            # be named `delete_channel()`. If that were the case, we would do the
            # following cleanup from that new `delete_channel()` method, but, since
            # that's not how Django Channels works (yet), we do the cleanup below:
            if channel in self.channels:
                del self.channels[channel]
                try:
                    shard = self._get_shard(channel)
                    await shard.unsubscribe(channel)
                except Exception:
                    traceback.print_exc(file=sys.stderr)
                    # We don't re-raise here because we want the CancelledError to be the one re-raised.
            raise

        return msgpack.unpackb(message)

    ################################################################################
    # Groups extension
    ################################################################################

    async def group_add(self, group, channel):
        """
        Adds the channel name to a group.
        """
        if channel not in self.channels:
            raise RuntimeError(
                "You can only call group_add() on channels that exist in-process.\n"
                "Consumers are encouraged to use the common pattern:\n"
                f"   self.channel_layer.group_add({repr(group)}, self.channel_name)"
            )
        group_channel = self._get_group_channel_name(group)
        if group_channel not in self.groups:
            self.groups[group_channel] = set()
        group_channels = self.groups[group_channel]
        if channel not in group_channels:
            group_channels.add(channel)
        shard = self._get_shard(group_channel)
        await shard.subscribe(group_channel)

    async def group_discard(self, group, channel):
        """
        Removes the channel from a group.
        """
        group_channel = self._get_group_channel_name(group)
        assert group_channel in self.groups
        group_channels = self.groups[group_channel]
        assert channel in group_channels
        group_channels.remove(channel)
        if len(group_channels) == 0:
            del self.groups[group_channel]
            shard = self._get_shard(group_channel)
            await shard.unsubscribe(group_channel)

    async def group_send(self, group, message):
        """
        Send the message to all subscribers of the group.
        """
        group_channel = self._get_group_channel_name(group)
        shard = self._get_shard(group_channel)
        await shard.publish(group_channel, message)


def on_close_noop(sender, exc=None):
    """
    If you don't pass an `on_close` function to the `Receiver`, then it
    defaults to one that closes the Receiver whenever the last subscriber
    unsubscribes. That is not what we want; instead, we want the Receiver
    to continue even if no one is subscribed, because soon someone *will*
    subscribe and we want things to continue from there. Passing this
    empty function solves it.
    """
    pass


class RedisSingleShardConnection:
    def __init__(self, host, channel_layer):
        self.host = host
        self.channel_layer = channel_layer
        self._subscribed_to = set()
        self._lock = None
        self._pub_conn = None
        self._sub_conn = None
        self._receiver = None
        self._receive_task = None
        self._keepalive_task = None

    async def publish(self, channel, message):
        conn = await self._get_pub_conn()
        await conn.publish(channel, msgpack.packb(message))

    async def subscribe(self, channel):
        if channel not in self._subscribed_to:
            self._subscribed_to.add(channel)
            conn = await self._get_sub_conn()
            await conn.subscribe(self._receiver.channel(channel))

    async def unsubscribe(self, channel):
        if channel in self._subscribed_to:
            self._subscribed_to.remove(channel)
            conn = await self._get_sub_conn()
            await conn.unsubscribe(channel)

    async def _get_pub_conn(self):
        """
        Return the connection to this shard that is used for *publishing* messages.

        If the connection is dead, automatically reconnect.
        """
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._pub_conn is not None and self._pub_conn.closed:
                self._pub_conn = None
            if self._pub_conn is None:
                while True:
                    try:
                        self._pub_conn = await aioredis.create_redis(self.host)
                        break
                    except Exception:
                        print(
                            f"Failed to connect to Redis publish host: {self.host}; will try again in 1 second...",
                            file=sys.stderr,
                        )
                        await asyncio.sleep(1)
            return self._pub_conn

    async def _get_sub_conn(self):
        """
        Return the connection to this shard that is used for *subscribing* to channels.

        If the connection is dead, automatically reconnect and resubscribe to all our channels!
        """
        if self._keepalive_task is None:
            self._keepalive_task = asyncio.create_task(self._do_keepalive())
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._sub_conn is not None and self._sub_conn.closed:
                self._sub_conn = None
            if self._sub_conn is None:
                if self._receive_task is not None:
                    self._receive_task.cancel()
                    try:
                        await self._receive_task
                    except asyncio.CancelledError:
                        # This is the normal case, that `asyncio.CancelledError` is throw. All good.
                        pass
                    except Exception:
                        traceback.print_exc(file=sys.stderr)
                        # Don't re-raise here. We don't actually care why `_receive_task` didn't exit cleanly.
                    self._receive_task = None
                while True:
                    try:
                        self._sub_conn = await aioredis.create_redis(self.host)
                        break
                    except Exception:
                        print(
                            f"Failed to connect to Redis subscribe host: {self.host}; will try again in 1 second...",
                            file=sys.stderr,
                        )
                        await asyncio.sleep(1)
                self._receiver = aioredis.pubsub.Receiver(on_close=on_close_noop)
                self._receive_task = asyncio.create_task(self._do_receiving())
                if len(self._subscribed_to) > 0:
                    # Do our best to recover by resubscribing to the channels that we were previously subscribed to.
                    resubscribe_to = [
                        self._receiver.channel(name) for name in self._subscribed_to
                    ]
                    await self._sub_conn.subscribe(*resubscribe_to)
            return self._sub_conn

    async def _do_receiving(self):
        async for ch, message in self._receiver.iter():
            name = ch.name
            if isinstance(name, bytes):
                # Reversing what happens here:
                #   https://github.com/aio-libs/aioredis-py/blob/8a207609b7f8a33e74c7c8130d97186e78cc0052/aioredis/util.py#L17
                name = name.decode()
            if name in self.channel_layer.channels:
                self.channel_layer.channels[name].put_nowait(message)
            elif name in self.channel_layer.groups:
                for channel_name in self.channel_layer.groups[name]:
                    if channel_name in self.channel_layer.channels:
                        self.channel_layer.channels[channel_name].put_nowait(message)

    async def _do_keepalive(self):
        """
        This task's simple job is just to call `self._get_sub_conn()` periodically.

        Why? Well, calling `self._get_sub_conn()` has the nice side-affect that if
        that connection has died (because Redis was restarted, or there was a networking
        hiccup, for example), then calling `self._get_sub_conn()` will reconnect and
        restore our old subscriptions. Thus, we want to do this on a predictable schedule.
        This is kinda a sub-optimal way to achieve this, but I can't find a way in aioredis
        to get a notification when the connection dies. I find this (sub-optimal) method
        of checking the connection state works fine for my app; if Redis restarts, we reconnect
        and resubscribe *quickly enough*; I mean, Redis restarting is already bad because it
        will cause messages to get lost, and this periodic check at least minimizes the
        damage *enough*.

        Note you wouldn't need this if you were *sure* that there would be a lot of subscribe/
        unsubscribe events on your site, because such events each call `self._get_sub_conn()`.
        Thus, on a site with heavy traffic this task may not be necessary, but also maybe it is.
        Why? Well, in a heavy traffic site you probably have more than one Django server replicas,
        so it might be the case that one of your replicas is under-utilized and this periodic
        connection check will be beneficial in the same way as it is for a low-traffic site.
        """
        while True:
            await asyncio.sleep(1)
            try:
                await self._get_sub_conn()
            except Exception:
                traceback.print_exc(file=sys.stderr)

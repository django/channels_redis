from __future__ import unicode_literals

from asgi_redis import RedisChannelLayer


__version__ = pkg_resources.require('asgi_redis')[0].version


class RedisLocalChannelLayer(RedisChannelLayer):
    """
    Variant of the Redis channel layer that also uses a local-machine
    channel layer instance to route all non-machine-specific messages
    to a local machine, while using the Redis backend for all machine-specific
    messages and group management/sends.

    This allows the majority of traffic to go over the local layer for things
    like http.request and websocket.receive, while still allowing Groups to
    broadcast to all connected clients and keeping reply_channel names valid
    across all workers.
    """

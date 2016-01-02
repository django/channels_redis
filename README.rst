asgi_redis
==========

An ASGI channel layer that uses Redis as its backing store, and supports
both a single-server and sharded configurations, as well as group support.


Usage
-----

You'll need to instantiate the channel layer with connection details, which
include:

* ``hosts``: The server(s) to connect to, as either URIs or ``(host, port)``
  tuples. Defaults to ``['localhost', 6379]``.
  Pass multiple hosts to enable sharding, but note that changing host
  list will lose most sharded data.
* ``prefix``: Prefix to add to all Redis keys. Defaults to ``asgi:``.
* ``expiry``: Message expiry in seconds. Defaults to ``60``.

Example::

    channel_layer = RedisChannelLayer(
        host="redis",
        db=4,
    )


TODO
----

* Expire/clean out groups
* Prune channel lists on write/periodically as well as on read
* Efficient ``send_group`` implementation with Lua

asgi_redis
==========

.. image:: https://api.travis-ci.org/andrewgodwin/asgi_redis.svg
    :target: https://travis-ci.org/andrewgodwin/asgi_redis

.. image:: https://img.shields.io/pypi/v/asgi_redis.svg
    :target: https://pypi.python.org/pypi/asgi_redis

An ASGI channel layer that uses Redis as its backing store, and supports
both a single-server and sharded configurations, as well as group support.


Usage
-----

You'll need to instantiate the channel layer with at least ``hosts``,
and other options if you need them.

Example::

    channel_layer = RedisChannelLayer(
        host="redis",
        db=4,
        channel_capacity={
            "http.request": 200,
            "http.response*": 10,
        }
    )

hosts
~~~~~

The server(s) to connect to, as either URIs or ``(host, port)`` tuples. Defaults to ``['localhost', 6379]``. Pass multiple hosts to enable sharding, but note that changing the host list will lose some sharded data.

prefix
~~~~~~

Prefix to add to all Redis keys. Defaults to ``asgi:``. If you're running
two or more entirely separate channel layers through the same Redis instance,
make sure they have different prefixes. All servers talking to the same layer
should have the same prefix, though.

expiry
~~~~~~

Message expiry in seconds. Defaults to ``60``. You generally shouldn't need
to change this, but you may want to turn it down if you have peaky traffic you
wish to drop, or up if you have peaky traffic you want to backlog until you
get to it.

group_expiry
~~~~~~~~~~~~

Group expiry in seconds. Defaults to ``86400``. Interface servers will drop
connections after this amount of time; it's recommended you reduce it for a
healthier system that encourages disconnections.

capacity
~~~~~~~~

Default channel capacity. Defaults to ``100``. Once a channel is at capacity,
it will refuse more messages. How this affects different parts of the system
varies; a HTTP server will refuse connections, for example, while Django
sending a response will just wait until there's space.

channel_capacity
~~~~~~~~~~~~~~~~

Per-channel capacity configuration. This lets you tweak the channel capacity
based on the channel name, and supports both globbing and regular expressions.

It should be a dict mapping channel name pattern to desired capacity; if the
dict key is a string, it's intepreted as a glob, while if it's a compiled
``re`` object, it's treated as a regular expression.

This example sets ``http.request`` to 200, all ``http.response!`` channels
to 10, and all ``websocket.send!`` channels to 20::

    channel_capacity={
        "http.request": 200,
        "http.response!*": 10,
        re.compile(r"^websocket.send\!.+"): 20,
    }

If you want to enforce a matching order, use an ``OrderedDict`` as the
argument; channels will then be matched in the order the dict provides them.

symmetric_encryption_keys
~~~~~~~~~~~~~~~~~~~~~~~~~

Pass this to enable the optional symmetric encryption mode of the backend. To
use it, make sure you have the ``cryptography`` package installed, or specify
the ``cryptography`` extra when you install ``asgi_redis``::

    pip install asgi_redis[cryptography]

``symmetric_encryption_keys`` should be a list of strings, with each string
being an encryption key. The first key is always used for encryption; all are
considered for decryption, so you can rotate keys without downtime - just add
a new key at the start and move the old one down, then remove the old one
after the message expiry time has passed.

Keys **should have at least 32 bytes of entropy** - they are passed through
the SHA256 hash function before being used as an encryption key. Any string
will work, but the shorter the string, the easier the encryption is to break.

If you're using Django, you may also wish to set this to your site's
``SECRET_KEY`` setting via the ``CHANNEL_LAYERS`` setting.


TODO
----

* Efficient ``send_group`` implementation with Lua

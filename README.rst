channels_redis
==============

.. image:: https://api.travis-ci.org/django/channels_redis.svg
    :target: https://travis-ci.org/django/channels_redis

.. image:: https://img.shields.io/pypi/v/channels_redis.svg
    :target: https://pypi.python.org/pypi/channels_redis

A Django Channels channel layer that uses Redis as its backing store, and supports
both a single-server and sharded configurations, as well as group support. Requires
Python 3.6 or higher to function correctly (3.5 will look like it works, but have
random errors).

**Note:** Prior versions of this package were called ``asgi_redis`` and are
still available under PyPI as that name if you need them for Channels 1.x projects.
This package is for Channels 2 projects only.


Usage
-----

Set up the channel layer in your Django settings file like so::

    CHANNEL_LAYERS = {
        "default": {
            "BACKEND": "channels_redis.core.RedisChannelLayer",
            "CONFIG": {
                "hosts": [("localhost", 6379)],
            },
        },
    }

Possible options for ``CONFIG`` are listed below.

``hosts``
~~~~~~~~~

The server(s) to connect to, as either URIs, ``(host, port)`` tuples, or dicts conforming to `create_connection <https://aioredis.readthedocs.io/en/v1.1.0/api_reference.html#aioredis.create_connection>`_.
Defaults to ``['localhost', 6379]``. Pass multiple hosts to enable sharding,
but note that changing the host list will lose some sharded data.

If your server is listening on a UNIX domain socket, you can also use that to connect: ``["unix:///path/to/redis.sock"]``.
This should be slightly faster than a loopback TCP connection.

``prefix``
~~~~~~~~~~

Prefix to add to all Redis keys. Defaults to ``asgi:``. If you're running
two or more entirely separate channel layers through the same Redis instance,
make sure they have different prefixes. All servers talking to the same layer
should have the same prefix, though.

``expiry``
~~~~~~~~~~

Message expiry in seconds. Defaults to ``60``. You generally shouldn't need
to change this, but you may want to turn it down if you have peaky traffic you
wish to drop, or up if you have peaky traffic you want to backlog until you
get to it.

``group_expiry``
~~~~~~~~~~~~~~~~

Group expiry in seconds. Defaults to ``86400``. Channels will be removed
from the group after this amount of time; it's recommended you reduce it
for a healthier system that encourages disconnections. This value should
not be lower than the relevant timeouts in the interface server (e.g.
the ``--websocket_timeout`` to `daphne
<https://github.com/django/daphne>`_).

``capacity``
~~~~~~~~~~~~

Default channel capacity. Defaults to ``100``. Once a channel is at capacity,
it will refuse more messages. How this affects different parts of the system
varies; a HTTP server will refuse connections, for example, while Django
sending a response will just wait until there's space.

``channel_capacity``
~~~~~~~~~~~~~~~~~~~~

Per-channel capacity configuration. This lets you tweak the channel capacity
based on the channel name, and supports both globbing and regular expressions.

It should be a dict mapping channel name pattern to desired capacity; if the
dict key is a string, it's intepreted as a glob, while if it's a compiled
``re`` object, it's treated as a regular expression.

This example sets ``http.request`` to 200, all ``http.response!`` channels
to 10, and all ``websocket.send!`` channels to 20:

.. code-block:: python

    CHANNEL_LAYERS = {
        "default": {
            "BACKEND": "channels_redis.core.RedisChannelLayer",
            "CONFIG": {
                "hosts": [("localhost", 6379)],
                "channel_capacity": {
                    "http.request": 200,
                    "http.response!*": 10,
                    re.compile(r"^websocket.send\!.+"): 20,
                },
            },
        },
    }

If you want to enforce a matching order, use an ``OrderedDict`` as the
argument; channels will then be matched in the order the dict provides them.

``symmetric_encryption_keys``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pass this to enable the optional symmetric encryption mode of the backend. To
use it, make sure you have the ``cryptography`` package installed, or specify
the ``cryptography`` extra when you install ``channels_redis``::

    pip install channels_redis[cryptography]

``symmetric_encryption_keys`` should be a list of strings, with each string
being an encryption key. The first key is always used for encryption; all are
considered for decryption, so you can rotate keys without downtime - just add
a new key at the start and move the old one down, then remove the old one
after the message expiry time has passed.

Data is encrypted both on the wire and at rest in Redis, though we advise
you also route your Redis connections over TLS for higher security; the Redis
protocol is still unencrypted, and the channel and group key names could
potentially contain metadata patterns of use to attackers.

Keys **should have at least 32 bytes of entropy** - they are passed through
the SHA256 hash function before being used as an encryption key. Any string
will work, but the shorter the string, the easier the encryption is to break.

If you're using Django, you may also wish to set this to your site's
``SECRET_KEY`` setting via the ``CHANNEL_LAYERS`` setting:

.. code-block:: python

    CHANNEL_LAYERS = {
        "default": {
            "BACKEND": "channels_redis.core.RedisChannelLayer",
            "CONFIG": {
                "hosts": ["redis://:password@127.0.0.1:6379/0"],
                "symmetric_encryption_keys": [SECRET_KEY],
            },
        },
    }


Dependencies
------------

Redis >= 5.0 is required for `channels_redis`. It supports Python 3.5.2 and up
(3.5.0 or 3.5.1 will not work due to our dependency, ``aioredis``).

Contributing
------------

Please refer to the
`main Channels contributing docs <https://github.com/django/channels/blob/master/CONTRIBUTING.rst>`_.
That also contains advice on how to set up the development environment and run the tests.

Maintenance and Security
------------------------

To report security issues, please contact security@djangoproject.com. For GPG
signatures and more security process information, see
https://docs.djangoproject.com/en/dev/internals/security/.

To report bugs or request new features, please open a new GitHub issue.

This repository is part of the Channels project. For the shepherd and maintenance team, please see the
`main Channels readme <https://github.com/django/channels/blob/master/README.rst>`_.

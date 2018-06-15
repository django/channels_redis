channels_redis_persist
======================

.. image:: https://api.travis-ci.org/genialis/channels_redis_persist.svg
    :target: https://travis-ci.org/genialis/channels_redis_persist

.. image:: https://img.shields.io/pypi/v/channels_redis_persist.svg
    :target: https://pypi.org/project/channels_redis_persist

A (hopefully) short-lived fork of Django Channels' `channels_redis`_ package
that adds support for persistent connections to Redis.

We plan on working with upstream to merge these changes back into
`channels_redis`_. If you want to follow the progress, see:

- `upstream issue report on reconnecting every time`_
- `upstream pull request adding persistence to connections`_

**Note:** Since this is meant to be a short-lived fork of channels_redis
package, we didn't rename the Python package (i.e. it is still called
``channels_redis``).

For all other information, refer to to the `channels_redis`_ package's README.

.. _channels_redis: https://github.com/django/channels_redis/
.. _upstream issue report on reconnecting every time:
  https://github.com/django/channels_redis/issues/100
.. _upstream pull request adding persistence to connections:
  https://github.com/django/channels_redis/pull/104

Note to developers
------------------

We plan on following `channels_redis`_'s releases and augmenting them with the
``.post<n>`` post release suffix.
For example, channels_redis_persist versions ``2.2.1.post1``, ``2.2.1.post2``,
... will correspond to channels_redis version ``2.2.1`` with patches adding
persistent connections support applied on top.

**Note:** The commits on the ``master`` branch will not be *stable*, i.e. they
will be continuously rebased on top of channels_redis's ``master`` branch.

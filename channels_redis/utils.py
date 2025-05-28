import binascii
import types
from redis import asyncio as aioredis
from typing import TYPE_CHECKING, Dict, List, Union, Any, Iterable

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer

    from redis.asyncio.connection import ConnectionPool
    from redis.asyncio.client import Redis
    from redis.asyncio import sentinel as redis_sentinel
    from asyncio import AbstractEventLoop

    from .pubsub import RedisPubSubChannelLayer
    from .core import RedisChannelLayer


def _consistent_hash(value: "Union[str, ReadableBuffer]", ring_size: int) -> int:
    """
    Maps the value to a node value between 0 and 4095
    using CRC, then down to one of the ring nodes.
    """
    if ring_size == 1:
        # Avoid the overhead of hashing and modulo when it is unnecessary.
        return 0

    if isinstance(value, str):
        value = value.encode("utf8")
    bigval = binascii.crc32(value) & 0xFFF
    ring_divisor = 4096 / float(ring_size)
    return int(bigval / ring_divisor)


def _wrap_close(
    proxy: "Union[RedisPubSubChannelLayer, RedisChannelLayer]",
    loop: "AbstractEventLoop",
):
    original_impl = loop.close

    def _wrapper(self, *args, **kwargs):
        if loop in proxy._layers:
            layer = proxy._layers[loop]
            del proxy._layers[loop]
            loop.run_until_complete(layer.flush())

        self.close = original_impl
        return self.close(*args, **kwargs)

    loop.close = types.MethodType(_wrapper, loop)


async def _close_redis(connection: "Redis"):
    """
    Handle compatibility with redis-py 4.x and 5.x close methods
    """
    try:
        await connection.aclose(close_connection_pool=True)
    except AttributeError:
        await connection.close(close_connection_pool=True)


def decode_hosts(
    hosts: "Union[Iterable, str, bytes, None]",
) -> "List[Dict]":
    """
    Takes the value of the "hosts" argument and returns
    a list of kwargs to use for the Redis connection constructor.
    """
    # If no hosts were provided, return a default value
    if not hosts:
        return [{"address": "redis://localhost:6379"}]
    # If they provided just a string, scold them.
    if isinstance(hosts, (str, bytes)):
        raise ValueError(
            "You must pass a list of Redis hosts, even if there is only one."
        )

    # Decode each hosts entry into a kwargs dict
    result: "List[Dict]" = []
    for entry in hosts:
        if isinstance(entry, dict):
            result.append(entry)
        elif isinstance(entry, (tuple, list)):
            result.append({"host": entry[0], "port": entry[1]})
        else:
            result.append({"address": entry})
    return result


def create_pool(host: "Dict[str, Any]") -> "ConnectionPool":
    """
    Takes the value of the "host" argument and returns a suited connection pool to
    the corresponding redis instance.
    """
    # avoid side-effects from modifying host
    host = host.copy()
    if "address" in host:
        address = host.pop("address")
        return aioredis.ConnectionPool.from_url(address, **host)

    master_name = host.pop("master_name", None)
    if master_name is not None:
        sentinels = host.pop("sentinels")
        sentinel_kwargs = host.pop("sentinel_kwargs", None)
        return redis_sentinel.SentinelConnectionPool(
            master_name,
            redis_sentinel.Sentinel(sentinels, sentinel_kwargs=sentinel_kwargs),
            **host,
        )

    return aioredis.ConnectionPool(**host)

import pytest

from channels_redis.utils import _consistent_hash, decode_hosts


@pytest.mark.parametrize(
    "value,ring_size,expected",
    [
        ("key_one", 1, 0),
        ("key_two", 1, 0),
        ("key_one", 2, 1),
        ("key_two", 2, 0),
        ("key_one", 10, 6),
        ("key_two", 10, 4),
        (b"key_one", 10, 6),
        (b"key_two", 10, 4),
    ],
)
def test_consistent_hash_result(value, ring_size, expected):
    assert _consistent_hash(value, ring_size) == expected


def test_decode_hosts_defaults_socket_timeout_to_none():
    assert decode_hosts(None) == [
        {"address": "redis://localhost:6379", "socket_timeout": None}
    ]
    assert decode_hosts(["redis://localhost:6379"]) == [
        {"address": "redis://localhost:6379", "socket_timeout": None}
    ]
    assert decode_hosts([("localhost", 6379)]) == [
        {"host": "localhost", "port": 6379, "socket_timeout": None}
    ]


def test_decode_hosts_preserves_explicit_socket_timeout():
    hosts = [{"address": "redis://localhost:6379", "socket_timeout": 10}]

    assert decode_hosts(hosts) == [
        {"address": "redis://localhost:6379", "socket_timeout": 10}
    ]


def test_decode_hosts_does_not_mutate_host_dicts():
    hosts = [{"address": "redis://localhost:6379"}]

    assert decode_hosts(hosts) == [
        {"address": "redis://localhost:6379", "socket_timeout": None}
    ]
    assert hosts == [{"address": "redis://localhost:6379"}]

import pytest

from channels_redis.utils import _consistent_hash


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

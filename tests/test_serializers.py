import pytest

from channels_redis.serializers import (
    JSONSerializer,
    MsgPackSerializer,
    SerializerDoesNotExist,
    SerializersRegistry,
)


@pytest.fixture
def registry():
    return SerializersRegistry()


class OnlySerialize:
    def serialize(self, message):
        return message


class OnlyDeserialize:
    def deserialize(self, message):
        return message


def bad_serializer():
    pass


class NoopSerializer:
    def serialize(self, message):
        return message

    def deserialize(self, message):
        return message


@pytest.mark.parametrize(
    "serializer_class", (OnlyDeserialize, OnlySerialize, bad_serializer)
)
def test_refuse_to_register_bad_serializers(registry, serializer_class):
    with pytest.raises(AssertionError):
        registry.register_serializer("custom", serializer_class)


def test_raise_error_for_unregistered_serializer(registry):
    with pytest.raises(SerializerDoesNotExist):
        registry.get_serializer("unexistent")


def test_register_custom_serializer(registry):
    registry.register_serializer("custom", NoopSerializer)
    serializer = registry.get_serializer("custom")
    assert serializer.serialize("message") == "message"
    assert serializer.deserialize("message") == "message"


@pytest.mark.parametrize(
    "serializer_cls,expected",
    (
        (MsgPackSerializer, b"\x83\xa1a\xc3\xa1b\xc0\xa1c\x81\xa1d\x90"),
        (JSONSerializer, b'{"a": true, "b": null, "c": {"d": []}}'),
    ),
)
@pytest.mark.parametrize("prefix_length", (8, 12, 0, -1))
def test_serialize(serializer_cls, expected, prefix_length):
    """
    Test default serialization method
    """
    message = {"a": True, "b": None, "c": {"d": []}}
    serializer = serializer_cls(random_prefix_length=prefix_length)
    serialized = serializer.serialize(message)
    assert isinstance(serialized, bytes)
    if prefix_length > 0:
        assert serialized[prefix_length:] == expected
    else:
        assert serialized == expected


@pytest.mark.parametrize(
    "serializer_cls,value",
    (
        (MsgPackSerializer, b"\x83\xa1a\xc3\xa1b\xc0\xa1c\x81\xa1d\x90"),
        (JSONSerializer, b'{"a": true, "b": null, "c": {"d": []}}'),
    ),
)
@pytest.mark.parametrize(
    "prefix_length,prefix",
    (
        (8, b"Q\x0c\xbb?Q\xbc\xe3|"),
        (12, b"Q\x0c\xbb?Q\xbc\xe3|D\xfd9\x00"),
        (0, b""),
        (-1, b""),
    ),
)
def test_deserialize(serializer_cls, value, prefix_length, prefix):
    """
    Test default deserialization method
    """
    message = prefix + value
    serializer = serializer_cls(random_prefix_length=prefix_length)
    deserialized = serializer.deserialize(message)
    assert isinstance(deserialized, dict)
    assert deserialized == {"a": True, "b": None, "c": {"d": []}}


@pytest.mark.parametrize(
    "serializer_cls,clear_value",
    (
        (MsgPackSerializer, b"\x83\xa1a\xc3\xa1b\xc0\xa1c\x81\xa1d\x90"),
        (JSONSerializer, b'{"a": true, "b": null, "c": {"d": []}}'),
    ),
)
def test_serialization_encrypted(serializer_cls, clear_value):
    """
    Test serialization rount-trip with encryption
    """
    message = {"a": True, "b": None, "c": {"d": []}}
    serializer = serializer_cls(
        symmetric_encryption_keys=["a-test-key"], random_prefix_length=4
    )
    serialized = serializer.serialize(message)
    assert isinstance(serialized, bytes)
    assert serialized[4:] != clear_value
    deserialized = serializer.deserialize(serialized)
    assert isinstance(deserialized, dict)
    assert deserialized == message

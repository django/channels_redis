import abc
import base64
import hashlib
import json
import random
import typing

if typing.TYPE_CHECKING:
    from typing_extensions import Buffer

try:
    from cryptography.fernet import Fernet, MultiFernet

    _MultiFernet = MultiFernet
    _Fernet = Fernet
except ImportError:
    _MultiFernet = _Fernet = None


class SerializerDoesNotExist(KeyError):
    """The requested serializer was not found."""


class BaseMessageSerializer(abc.ABC):
    def __init__(
        self,
        symmetric_encryption_keys: typing.Optional[
            typing.Iterable[typing.Union[str, "Buffer"]]
        ] = None,
        random_prefix_length: int = 0,
        expiry: typing.Optional[int] = None,
    ):
        self.random_prefix_length = random_prefix_length
        self.expiry = expiry
        self.crypter: typing.Optional["MultiFernet"] = None
        # Set up any encryption objects
        self._setup_encryption(symmetric_encryption_keys)

    def _setup_encryption(
        self,
        symmetric_encryption_keys: typing.Optional[
            typing.Union[typing.Iterable[typing.Union[str, "Buffer"]], str, bytes]
        ],
    ):
        # See if we can do encryption if they asked
        if not symmetric_encryption_keys:
            self.crypter = None
            return

        if isinstance(symmetric_encryption_keys, (str, bytes)):
            raise ValueError(
                "symmetric_encryption_keys must be a list of possible keys"
            )
        keys = typing.cast(
            typing.Iterable[typing.Union[str, "Buffer"]], symmetric_encryption_keys
        )
        if _MultiFernet is None:
            raise ValueError(
                "Cannot run with encryption without 'cryptography' installed."
            )
        sub_fernets = [self.make_fernet(key) for key in keys]
        self.crypter = _MultiFernet(sub_fernets)

    def make_fernet(self, key: typing.Union[str, "Buffer"]) -> "Fernet":
        """
        Given a single encryption key, returns a Fernet instance using it.
        """
        if _Fernet is None:
            raise ValueError(
                "Cannot run with encryption without 'cryptography' installed."
            )

        if isinstance(key, str):
            key = key.encode("utf-8")
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return _Fernet(formatted_key)

    @abc.abstractmethod
    def as_bytes(self, message, *args, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def from_bytes(self, message, *args, **kwargs):
        raise NotImplementedError

    def serialize(self, message) -> bytes:
        """
        Serializes message to a byte string.
        """
        message = self.as_bytes(message)
        if self.crypter:
            message = self.crypter.encrypt(message)

        if self.random_prefix_length > 0:
            # provide random prefix
            message = (
                random.getrandbits(8 * self.random_prefix_length).to_bytes(
                    self.random_prefix_length, "big"
                )
                + message
            )
        return message

    def deserialize(self, message: bytes):
        """
        Deserializes from a byte string.
        """
        if self.random_prefix_length > 0:
            # Removes the random prefix
            message = message[self.random_prefix_length :]  # noqa: E203

        if self.crypter:
            ttl = self.expiry if self.expiry is None else self.expiry + 10
            message = self.crypter.decrypt(message, ttl)
        return self.from_bytes(message)


class MissingSerializer(BaseMessageSerializer):
    exception: "Exception" = Exception(None)

    def __init__(self, *args, **kwargs):
        raise self.exception


class JSONSerializer(BaseMessageSerializer):
    # json module by default always produces str while loads accepts bytes
    # thus we must force bytes conversion
    # we use UTF-8 since it is the recommended encoding for interoperability
    # see https://docs.python.org/3/library/json.html#character-encodings
    def as_bytes(self, message, *args, **kwargs) -> bytes:
        message = json.dumps(message, *args, **kwargs)
        return message.encode("utf-8")

    from_bytes = staticmethod(json.loads)


# code ready for a future in which msgpack may become an optional dependency
MsgPackSerializer: typing.Optional[typing.Type[BaseMessageSerializer]] = None
try:
    import msgpack
except ImportError as exc:

    class _MsgPackSerializer(MissingSerializer):
        exception = exc

    MsgPackSerializer = _MsgPackSerializer
else:

    class __MsgPackSerializer(BaseMessageSerializer):
        as_bytes = staticmethod(msgpack.packb)
        from_bytes = staticmethod(msgpack.unpackb)

    MsgPackSerializer = __MsgPackSerializer


class SerializersRegistry:
    """
    Serializers registry inspired by that of ``django.core.serializers``.
    """

    def __init__(self):
        self._registry: typing.Dict[typing.Any, typing.Type[BaseMessageSerializer]] = {}

    def register_serializer(
        self, format, serializer_class: typing.Type[BaseMessageSerializer]
    ):
        """
        Register a new serializer for given format
        """
        assert isinstance(serializer_class, type) and (
            issubclass(serializer_class, BaseMessageSerializer)
            or (
                hasattr(serializer_class, "serialize")
                and hasattr(serializer_class, "deserialize")
            )
        ), """
            `serializer_class` should be a class which implements `serialize` and `deserialize` method
            or a subclass of `channels_redis.serializers.BaseMessageSerializer`
        """

        self._registry[format] = serializer_class

    def get_serializer(self, format, *args, **kwargs) -> BaseMessageSerializer:
        try:
            serializer_class = self._registry[format]
        except KeyError:
            raise SerializerDoesNotExist(format)

        return serializer_class(*args, **kwargs)


registry = SerializersRegistry()
registry.register_serializer("json", JSONSerializer)
registry.register_serializer("msgpack", MsgPackSerializer)

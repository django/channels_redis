import binascii
import types


def _consistent_hash(value, ring_size):
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


def _wrap_close(proxy, loop):
    original_impl = loop.close

    def _wrapper(self, *args, **kwargs):
        if loop in proxy._layers:
            layer = proxy._layers[loop]
            del proxy._layers[loop]
            loop.run_until_complete(layer.flush())

        self.close = original_impl
        return self.close(*args, **kwargs)

    loop.close = types.MethodType(_wrapper, loop)

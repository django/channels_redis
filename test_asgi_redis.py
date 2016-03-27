import unittest
from asgi_redis import RedisChannelLayer
from asgiref.conformance import ConformanceTestCase


# Default conformance tests
class RedisLayerTests(ConformanceTestCase):

    channel_layer = RedisChannelLayer(expiry=1, group_expiry=2)
    expiry_delay = 1.1

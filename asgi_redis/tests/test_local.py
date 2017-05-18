from __future__ import unicode_literals
from asgi_redis import RedisLocalChannelLayer
from asgiref.conformance import ConformanceTestCase

from .constants import REDIS_HOSTS


# Local layer conformance tests
class RedisLocalLayerTests(ConformanceTestCase):

    expiry_delay = 1.1
    capacity_limit = 5

    @classmethod
    def setUpClass(cls):
        super(RedisLocalLayerTests, cls).setUpClass()
        cls.channel_layer = RedisLocalChannelLayer(
            hosts=REDIS_HOSTS,
            expiry=1,
            group_expiry=2,
            capacity=5
        )

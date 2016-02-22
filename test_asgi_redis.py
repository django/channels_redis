from asgi_redis import RedisChannelLayer
from asgiref.conformance import make_tests

channel_layer = RedisChannelLayer(expiry=1)
RedisTests = make_tests(channel_layer, expiry_delay=1.1)

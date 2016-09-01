import pkg_resources
from .core import RedisChannelLayer, DjangoRedisChannelLayer
from .local import RedisLocalChannelLayer

__version__ = pkg_resources.require('asgi_redis')[0].version

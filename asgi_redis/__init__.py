import pkg_resources
from .core import RedisChannelLayer

__version__ = pkg_resources.require('asgi_redis')[0].version

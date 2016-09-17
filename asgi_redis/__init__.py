import pkg_resources
from .core import RedisChannelLayer
from .local import RedisLocalChannelLayer


def get_version():
    return pkg_resources.require('asgi_redis')[0].version

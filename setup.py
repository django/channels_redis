from os.path import dirname, join

from setuptools import find_packages, setup


def get_version():

    for line in open(join(dirname(__file__), 'asgi_redis', '__init__.py')):
        if line.startswith('__version__'):
            version_str = line.split('=')[-1].strip()
            version = version_str.replace('"', '').replace("'", '')
            return version


# We use the README as the long_description
readme = open(join(dirname(__file__), 'README.rst')).read()

crypto_requires = ['cryptography>=1.3.0']
twisted_requires = ['twisted>=17.1', 'txredisapi']
test_requires = crypto_requires + [
    'pytest>=3.0',
    'pytest-django>=3.0',
    'asgi_ipc',
    'channels>=1.1.0',
    'requests>=2.12',
    'websocket_client>=0.40',
]

setup(
    name='asgi_redis',
    version=get_version(),
    url='http://github.com/django/asgi_redis/',
    author='Django Software Foundation',
    author_email='foundation@djangoproject.com',
    description='Redis-backed ASGI channel layer implementation',
    long_description=readme,
    license='BSD',
    zip_safe=False,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    install_requires=[
        'six',
        'redis>=2.10',
        'msgpack-python',
        'asgiref~=1.1.2',
    ],
    extras_require={
        'cryptography': crypto_requires,
        'twisted': twisted_requires,
        'tests': crypto_requires + twisted_requires + test_requires,
    },
)

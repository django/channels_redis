import os
from setuptools import setup, find_packages


def get_version():
    for line in open(os.path.join(os.path.dirname(__file__), 'asgi_redis', '__init__.py')):
        if line.startswith('__version__'):
            return line.split('=')[-1].strip().replace('"', '').replace("'", '')

# We use the README as the long_description
readme_path = os.path.join(os.path.dirname(__file__), "README.rst")

crypto_requires = ['cryptography>=1.3.0']
twisted_requires = ['twisted>=17.1', 'txredisapi']
tests_require = ['tox', 'asgi_ipc']


setup(
    name='asgi_redis',
    version=get_version(),
    url='http://github.com/django/asgi_redis/',
    author='Django Software Foundation',
    author_email='foundation@djangoproject.com',
    description='Redis-backed ASGI channel layer implementation',
    long_description=open(readme_path).read(),
    license='BSD',
    zip_safe=False,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'six',
        'redis>=2.10',
        'msgpack-python',
        'asgiref~=1.1.2',
    ],
    extras_require={
        "cryptography": crypto_requires,
        "twisted": twisted_requires,
        "tests": crypto_requires + twisted_requires + tests_require,
    }
)

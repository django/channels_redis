from os.path import dirname, join

from setuptools import find_packages, setup

from channels_redis import __version__

# We use the README as the long_description
readme = open(join(dirname(__file__), "README.rst")).read()

crypto_requires = ["cryptography>=1.3.0"]

test_requires = crypto_requires + [
    "pytest",
    "pytest-asyncio",
    "async-timeout",
    "pytest-timeout",
]


setup(
    name="channels_redis",
    version=__version__,
    url="http://github.com/django/channels_redis/",
    author="Django Software Foundation",
    author_email="foundation@djangoproject.com",
    description="Redis-backed ASGI channel layer implementation",
    long_description=readme,
    license="BSD",
    zip_safe=False,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    python_requires=">=3.9",
    install_requires=[
        "redis>=4.6",
        "msgpack~=1.0",
        "asgiref>=3.9.1,<4",
        "channels>=4.2.2",
    ],
    extras_require={"cryptography": crypto_requires, "tests": test_requires},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Framework :: Django",
        "Framework :: Django :: 4.2",
        "Framework :: Django :: 5.1",
        "Framework :: Django :: 5.2",
        "Framework :: Django :: 6.0",
        "Topic :: Internet :: WWW/HTTP",
    ],
)

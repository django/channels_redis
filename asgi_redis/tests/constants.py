import os

if os.environ.get("DOCKER_TEST_ENV"):
    REDIS_HOSTS = [("redis-master-1", 6379), ("redis-master-2", 6379)]
    SERVICE_NAMES = ["master-1", "master-2"]
    SENTINEL_HOSTS = [("sentinel", 26379)]
else:
    REDIS_HOSTS = [("localhost", 6379)]
    SERVICE_NAMES = []
    SENTINEL_HOSTS = []

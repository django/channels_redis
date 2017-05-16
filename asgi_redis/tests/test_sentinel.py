from __future__ import unicode_literals
import time
import unittest
import redis.sentinel
from redis.sentinel import MasterNotFoundError
from asgi_redis import RedisSentinelChannelLayer
from asgiref.conformance import ConformanceTestCase

service_names = ["master-1", "master-2"]
sentinel_hosts = [("sentinel", 26379)]


def sentinel_exists():
    sen = redis.sentinel.Sentinel(sentinel_hosts)
    try:
        sen.discover_master(service_names[0])
    except MasterNotFoundError:
        return False
    return True


# Default conformance tests
@unittest.skipUnless(sentinel_exists(), "Redis sentinel not running")
class RedisLayerTests(ConformanceTestCase):

    channel_layer = RedisSentinelChannelLayer(
        hosts=sentinel_hosts,
        expiry=1,
        group_expiry=2,
        capacity=5,
        services=service_names
    )
    expiry_delay = 1.1
    receive_tries = 2

    # The functionality this test is for is not yet present (it's not required,
    # and will slow stuff down, so will be optional), but it's here for future reference.
    @unittest.expectedFailure
    def test_group_message_eviction(self):
        """
        Tests that when messages expire, group expiry also occurs.
        """
        # Add things to a group and send a message that should expire
        self.channel_layer.group_add("tgme_group", "tgme_test")
        self.channel_layer.send_group("tgme_group", {"value": "blue"})
        # Wait message expiry plus a tiny bit (must sum to less than group expiry)
        time.sleep(1.2)
        # Send new message to group, ensure message never arrives
        self.channel_layer.send_group("tgme_group", {"value": "blue"})
        channel, message = self.receive(["tgme_test"])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    def test_statistics(self):
        self.channel_layer.send("first_channel", {"pay": "load"})
        self.channel_layer.send("first_channel", {"pay": "load"})
        self.channel_layer.send("second_channel", {"pay": "load"})

        self.assertEqual(
            self.channel_layer.global_statistics(),
            {
                'messages_count': 3,
                'channel_full_count': 0,
            }
        )

        self.assertEqual(
            self.channel_layer.channel_statistics("first_channel"),
            {
                'messages_count': 2,
                'messages_pending': 2,
                'messages_max_age': 0,
                'channel_full_count': 0,
            }
        )

        self.assertEqual(
            self.channel_layer.channel_statistics("second_channel"),
            {
                'messages_count': 1,
                'messages_pending': 1,
                'messages_max_age': 0,
                'channel_full_count': 0,
            }
        )

    def test_channel_full_statistics(self):
        if self.capacity_limit is None:
            raise unittest.SkipTest("No test capacity specified")

        for _ in range(self.capacity_limit):
            self.channel_layer.send("first_channel", {"pay": "load"})

        for _ in range(4):
            with self.assertRaises(RedisSentinelChannelLayer.ChannelFull):
                self.channel_layer.send("first_channel", {"pay": "load"})

        # check that channel full exception are counted as such, not towards messages
        for _ in range(self.capacity_limit):
            self.channel_layer.send("first_channel", {"pay": "load"})

        for _ in range(4):
            with self.assertRaises(RedisSentinelChannelLayer.ChannelFull):
                self.channel_layer.send("first_channel", {"pay": "load"})

        # check that channel full exception are counted as such, not towards messages
        self.assertEqual(self.channel_layer.global_statistics()["channel_full_count"], 4)

        self.assertEqual(
            self.channel_layer.channel_statistics("first_channel")["channel_full_count"], 4)


# Encrypted variant of conformance tests
@unittest.skipUnless(sentinel_exists(), "Redis sentinel not running")
class EncryptedRedisLayerTests(ConformanceTestCase):

    channel_layer = RedisSentinelChannelLayer(
        hosts=sentinel_hosts,
        expiry=1,
        group_expiry=2,
        capacity=5,
        symmetric_encryption_keys=["test", "old"],
        services=service_names,
    )
    expiry_delay = 1.1
    receive_tries = 2


# Test that the backend can auto-discover masters from Sentinel
@unittest.skipUnless(sentinel_exists(), "Redis sentinel not running")
class AutoDiscoverRedisLayerTests(ConformanceTestCase):

    channel_layer = RedisSentinelChannelLayer(
        hosts=sentinel_hosts,
        expiry=1,
        group_expiry=2,
        capacity=5,
    )
    expiry_delay = 1.1
    receive_tries = 2

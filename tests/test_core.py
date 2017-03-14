from __future__ import unicode_literals
import time
import unittest
from asgi_redis import RedisChannelLayer
from asgiref.conformance import ConformanceTestCase

from .constants import REDIS_HOSTS


# Default conformance tests
class RedisLayerTests(ConformanceTestCase):

    expiry_delay = 1.1
    receive_tries = len(REDIS_HOSTS)

    @classmethod
    def setUpClass(cls):
        super(RedisLayerTests, cls).setUpClass()
        cls.channel_layer = RedisChannelLayer(hosts=REDIS_HOSTS, expiry=1, group_expiry=2, capacity=5)

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

    def test_received_message_deletion(self):
        """
        Ensures that when a message is received, the key containing its
        content is deleted as well (and not just left to EXPIRE to clean up)

        Note that this does not correctly fail on a sharded test, as it just
        runs key stats on one shard to verify. It will, however, always pass
        if things are working.
        """
        # Send and receive on the channel first to make the channel key
        self.channel_layer.send("test-deletion", {"first": True})
        self.receive(["test-deletion"])
        # Get the number of keys in the Redis database before we send
        num_keys = self.channel_layer.connection(0).dbsize()
        # Send and receive
        self.channel_layer.send("test-deletion", {"big": False})
        self.receive(["test-deletion"])
        # Verify the database did not grow in size
        self.assertEqual(num_keys, self.channel_layer.connection(0).dbsize())

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
            with self.assertRaises(RedisChannelLayer.ChannelFull):
                self.channel_layer.send("first_channel", {"pay": "load"})

        # check that channel full exception are counted as such, not towards messages
        self.assertEqual(self.channel_layer.global_statistics()["channel_full_count"], 4)

        self.assertEqual(
            self.channel_layer.channel_statistics("first_channel")["channel_full_count"], 4)


# Encrypted variant of conformance tests
class EncryptedRedisLayerTests(ConformanceTestCase):

    expiry_delay = 1.1
    receive_tries = len(REDIS_HOSTS)

    @classmethod
    def setUpClass(cls):
        super(EncryptedRedisLayerTests, cls).setUpClass()
        cls.channel_layer = RedisChannelLayer(
            hosts=REDIS_HOSTS,
            expiry=1,
            group_expiry=2,
            capacity=5,
            symmetric_encryption_keys=["test", "old"],
        )

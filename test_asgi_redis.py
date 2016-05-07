from __future__ import unicode_literals
import time
import unittest
from asgi_redis import RedisChannelLayer
from asgiref.conformance import ConformanceTestCase


# Default conformance tests
class RedisLayerTests(ConformanceTestCase):

    channel_layer = RedisChannelLayer(expiry=1, group_expiry=2, capacity=5)
    expiry_delay = 1.1
    capacity_limit = 5

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
        channel, message = self.channel_layer.receive_many(["tgme_test"])
        self.assertIs(channel, None)
        self.assertIs(message, None)

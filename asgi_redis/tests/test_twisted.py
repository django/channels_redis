from __future__ import unicode_literals
import unittest

# Twisted tests are skipped in setUpClass below if Twisted isn't present.
try:
    from twisted.internet import defer, reactor
    import twisted.trial.unittest
except ImportError:
    pass


from .test_core import RedisChannelLayer
from .constants import REDIS_HOSTS


class TwistedTests(twisted.trial.unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            import twisted
        except ImportError:
            raise unittest.SkipTest('Twisted not present, skipping Twisted tests.')

    def setUp(self):
        super(TwistedTests, self).setUp()
        self.channel_layer = RedisChannelLayer(hosts=REDIS_HOSTS, expiry=1, group_expiry=2, capacity=5)

    @defer.inlineCallbacks
    def test_receive_twisted(self):
        self.channel_layer.send("sr_test", {"value": "blue"})
        self.channel_layer.send("sr_test", {"value": "green"})
        self.channel_layer.send("sr_test2", {"value": "red"})
        # Get just one first
        channel, message = yield self.channel_layer.receive_twisted(["sr_test"])
        self.assertEqual(channel, "sr_test")
        self.assertEqual(message, {"value": "blue"})
        # And the second
        channel, message = yield self.channel_layer.receive_twisted(["sr_test"])
        self.assertEqual(channel, "sr_test")
        self.assertEqual(message, {"value": "green"})
        # And the other channel with multi select
        channel, message = yield self.channel_layer.receive_twisted(["sr_test", "sr_test2"])
        self.assertEqual(channel, "sr_test2")
        self.assertEqual(message, {"value": "red"})

    def tearDown(self):
        del self.channel_layer
        reactor.removeAll()
        super(TwistedTests, self).tearDown()

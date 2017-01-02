"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

from twisted.trial import unittest

from run_test_vortex_server import setupVortexServer
from vortex.VortexClient import VortexClient


class VortexTest(unittest.TestCase):
    def setUp(self):
        self.port = setupVortexServer()

    def testVortexConnect(self):
        client = VortexClient("unittest")
        return client.connect("127.0.0.1", self.port)

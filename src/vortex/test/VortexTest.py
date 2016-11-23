"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

from twisted.trial import unittest

from vortex.VortexClient import VortexClient
from vortex.test.SiteTest import setupVortexServer


class VortexTest(unittest.TestCase):
    def setUp(self):
        self.port = setupVortexServer()

    def testVortexConnect(self):
        client = VortexClient()
        return client.connect("127.0.0.1", self.port)

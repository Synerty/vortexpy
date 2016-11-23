"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

from PayloadEndpoint import PayloadEndpoint
from PayloadIO import PayloadIO
from twisted.trial import unittest

from Payload import Payload
from Vortex import Vortex


class PayloadEndpointPyTestAssignPayload():
    def __init__(self, unitTest):
        self._ut = unitTest

    def process(self, payload, **kwargs):
        self._ut.deliveredPayload = payload


class PayloadEndpointPyTest(unittest.TestCase):
    def setUp(self):
        PayloadIO._instance = None
        Vortex._instance = None

        self.deliveredPayload = None

    def _testBuild(self, plFilt, epFilt):
        payload = Payload()
        payload.filt.update(plFilt)
        for x in range(6):
            payload.filt['%s' % x] = x

        def processPayload(payload, **kwargs):
            self.deliveredPayload = payload

        self._keepFuncInScope = processPayload

        PayloadEndpoint(epFilt, processPayload)

        PayloadIO().process(payload)

        return payload

    def testFiltMatches(self):
        plFilt = {'This matches': 555}
        epFilt = {'This matches': 555}

        payload = self._testBuild(plFilt, epFilt)

        self.assertEqual(payload, self.deliveredPayload,
                         'PayloadIO/PayloadEndpoint delivery error')

    def testFiltValueUnmatched(self):
        plFilt = {'This matches': 555}
        epFilt = {'This matches': 0}

        self._testBuild(plFilt, epFilt)

        self.assertEqual(self.deliveredPayload, None,
                         'PayloadIO/PayloadEndpoint unmatched value test error')

    def testFiltKeyUnmatched(self):
        plFilt = {'This matches': 555}
        epFilt = {'This doesnt matches': 555}

        self._testBuild(plFilt, epFilt)

        self.assertEqual(self.deliveredPayload, None,
                         'PayloadIO/PayloadEndpoint unmatched value test error')

    def testFunctionGoesOutOfScope(self):
        filt = {'This matches': 555}

        payload = Payload()
        payload.filt = filt

        def subScope():
            def outOfScopeFunc(payload, **kwargs):
                self.deliveredPayload = payload

            PayloadEndpoint(filt, outOfScopeFunc)

        PayloadIO().process(payload)

        self.assertEqual(self.deliveredPayload, None,
                         'PayloadIO/PayloadEndpoint unmatched value test error')

    def testClassGoesOutOdScope(self):
        filt = {'This matches': 555}

        payload = Payload()
        payload.filt = filt

        def subScope():
            inst = PayloadEndpointPyTestAssignPayload(self)
            PayloadEndpoint(filt, inst.process)

        PayloadIO().process(payload)

        self.assertEqual(self.deliveredPayload, None,
                         'PayloadIO/PayloadEndpoint unmatched value test error')

    def testClassStaysInScope(self):
        filt = {'This matches': 555}

        payload = Payload()
        payload.filt = filt

        inst = PayloadEndpointPyTestAssignPayload(self)
        PayloadEndpoint(filt, inst.process)

        PayloadIO().process(payload)

        self.assertEqual(self.deliveredPayload, payload,
                         'PayloadIO/PayloadEndpoint unmatched value test error')

"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

from twisted.trial import unittest

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadIO import PayloadIO


class PayloadEndpointPyTestAssignPayload:
    def __init__(self, unitTest):
        self._ut = unitTest

    def process(self, payloadEnvelope: PayloadEnvelope, **kwargs):
        self._ut.deliveredPayload = payloadEnvelope


class PayloadEndpointPyTest(unittest.TestCase):
    def setUp(self):
        PayloadIO._instance = None

        self.deliveredPayloadEnvelope = None

    def _testBuild(self, plFilt, epFilt):
        payload = Payload()
        payload.filt.update(plFilt)
        for x in range(6):
            payload.filt["%s" % x] = x

        def processPayload(payloadEnvelope: PayloadEnvelope, **kwargs):
            self.deliveredPayloadEnvelope = payloadEnvelope

        self._keepFuncInScope = processPayload

        PayloadEndpoint(epFilt, processPayload)

        PayloadIO().process(
            payloadEnvelope=payload.makePayloadEnvelope(),
            vortexUuid="test",
            vortexName="test",
            httpSession=None,
            sendResponse=lambda x: x,
        )

        return payload

    def testFiltMatches(self):
        plFilt = {"key": "unittest", "This matches": 555}
        epFilt = {"key": "unittest", "This matches": 555}

        payload = self._testBuild(plFilt, epFilt)

        self.assertEqual(
            payload,
            self.deliveredPayloadEnvelope,
            "PayloadIO/PayloadEndpoint delivery error",
        )

    def testFiltValueUnmatched(self):
        plFilt = {"key": "unittest", "This matches": 555}
        epFilt = {"key": "unittest", "This matches": 0}

        self._testBuild(plFilt, epFilt)

        self.assertEqual(
            self.deliveredPayloadEnvelope,
            None,
            "PayloadIO/PayloadEndpoint unmatched value test error",
        )

    def testFiltKeyUnmatched(self):
        plFilt = {"key": "unittest", "This matches": 555}
        epFilt = {"key": "unittest", "This doesnt matches": 555}

        self._testBuild(plFilt, epFilt)

        self.assertEqual(
            self.deliveredPayloadEnvelope,
            None,
            "PayloadIO/PayloadEndpoint unmatched value test error",
        )

    def testFunctionGoesOutOfScope(self):
        filt = {"key": "unittest", "This matches": 555}

        payload = Payload()
        payload.filt = filt

        def subScope():
            def outOfScopeFunc(
                payloadEnvelope: PayloadEnvelope, *args, **kwargs
            ):
                self.deliveredPayloadEnvelope = payloadEnvelope

            PayloadEndpoint(filt, outOfScopeFunc)

        PayloadIO().process(
            payloadEnvelope=payload.makePayloadEnvelope(),
            vortexUuid="test",
            vortexName="test",
            httpSession=None,
            sendResponse=lambda x: x,
        )

        self.assertEqual(
            self.deliveredPayloadEnvelope,
            None,
            "PayloadIO/PayloadEndpoint unmatched value test error",
        )

    def testClassGoesOutOdScope(self):
        filt = {"key": "unittest", "This matches": 555}

        payload = Payload()
        payload.filt = filt

        def subScope():
            inst = PayloadEndpointPyTestAssignPayload(self)
            PayloadEndpoint(filt, inst.process)

        PayloadIO().process(
            payloadEnvelope=payload.makePayloadEnvelope(),
            vortexUuid="test",
            vortexName="test",
            httpSession=None,
            sendResponse=lambda x: x,
        )

        self.assertEqual(
            self.deliveredPayloadEnvelope,
            None,
            "PayloadIO/PayloadEndpoint unmatched value test error",
        )

    def testClassStaysInScope(self):
        filt = {"key": "unittest", "This matches": 555}

        payload = Payload()
        payload.filt = filt

        inst = PayloadEndpointPyTestAssignPayload(self)
        PayloadEndpoint(filt, inst.process)

        PayloadIO().process(
            payloadEnvelope=payload.makePayloadEnvelope(),
            vortexUuid="test",
            vortexName="test",
            httpSession=None,
            sendResponse=lambda x: x,
        )

        self.assertEqual(
            self.deliveredPayloadEnvelope,
            payload,
            "PayloadIO/PayloadEndpoint unmatched value test error",
        )

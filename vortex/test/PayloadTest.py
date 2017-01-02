"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import os
from twisted.trial import unittest

from vortex.Payload import Payload
from vortex.Tuple import TupleHash
from vortex.PayloadFilterKeys import rapuiClientEcho


def makeTestPayloadA():
    payload = Payload()
    payload.filt[rapuiClientEcho] = None
    return payload


class PayloadPyTest(unittest.TestCase):
    PERF_TEST_COUNT = 10000

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testPayloadToFromVortexMsg(self):
        # Create Payload
        origPayload = makeTestPayloadA()

        # To VortexServer Message
        vortexMsg = origPayload.toVortexMsg()
        # print vortexMsg

        payload = Payload().fromVortexMsg(vortexMsg)

        self.assertEqual(TupleHash(payload), TupleHash(payload))
        self.assertEqual(TupleHash(origPayload), TupleHash(origPayload))

        self.assertEqual(TupleHash(origPayload), TupleHash(payload),
                         'Payload serialise -> deserialise error')

    def testPayloadToFromJson(self):
        # Create Payload
        origPayload = makeTestPayloadA()

        # To JSON
        jsonStr = origPayload._toJson()
        # print vortexMsg
        print(jsonStr)

        payload = Payload()._fromJson(jsonStr)

        self.assertEqual(TupleHash(payload), TupleHash(payload))
        self.assertEqual(TupleHash(origPayload), TupleHash(origPayload))

        self.assertEqual(TupleHash(origPayload), TupleHash(payload),
                         'Payload serialise -> deserialise error')


    def testPayloadJsonPerformanceSingle(self):
        origPayload = makeTestPayloadA()
        for _ in range(self.PERF_TEST_COUNT):
            origPayload.tuples.append(makeTestPayloadA())
        jsonStr = origPayload._toJson()
        payload = Payload()._fromJson(jsonStr)

    def testPayloadVortexMsgPerformanceSingle(self):
        origPayload = makeTestPayloadA()
        for x in range(self.PERF_TEST_COUNT):
            origPayload.tuples.append(makeTestPayloadA())
        vortexMsg = origPayload.toVortexMsg()
        payload = Payload().fromVortexMsg(vortexMsg)

    def testPayloadJsonPerformanceMultiple(self):
        origPayload = makeTestPayloadA()
        for _ in range(self.PERF_TEST_COUNT):
            jsonStr = origPayload._toJson()
            payload = Payload()._fromJson(jsonStr)

    def testPayloadVortexMsgPerformanceMultiple(self):
        origPayload = makeTestPayloadA()
        for _ in range(self.PERF_TEST_COUNT):
            vortexMsg = origPayload.toVortexMsg()
            payload = Payload().fromVortexMsg(vortexMsg)


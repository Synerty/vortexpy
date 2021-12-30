"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import gc

from twisted.trial import unittest

from vortex.Payload import Payload
from vortex.PayloadFilterKeys import rapuiClientEcho
from vortex.Tuple import TupleHash
from vortex.test.VortexTcpMemoryLeakPayloadTest import MemoryCheckerTestMixin


def makeTestPayloadA(tupleDataSize=10 * 1024):
    payload = Payload()
    payload.filt[rapuiClientEcho] = None
    payload.tuples = ["12345678" * int(tupleDataSize / 8)]
    return payload


class PayloadPyTest(unittest.TestCase, MemoryCheckerTestMixin):
    PERF_TEST_COUNT = 10000

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # @profile
    def _makeStr(self, size):
        _ = ["12345678" * int(size / 8)]
        del _
        gc.collect()

    # @profile
    def _payloadEncodeDecode(self, size):
        origPayload = makeTestPayloadA(size)
        origPayload.date = None

        encodedPayload = origPayload.toEncodedPayload()
        payload = Payload().fromEncodedPayload(encodedPayload)

        self.assertEqual(payload.tuples[0], origPayload.tuples[0])

    def testMakeStr_100m_x100(self):
        self._memCheckFunction(self._makeStr, 1000, 100 * 1024 ** 2)

    def testPayloadToFromEncodedPayload_10k_x1000(self):
        self._memCheckFunction(self._payloadEncodeDecode, 1000, 10 * 1024)

    def testPayloadToFromEncodedPayload_100m_x100(self):
        self._memCheckFunction(self._payloadEncodeDecode, 100, 10 * 1024 ** 2)

    def testPayloadToFromEncodedPayload_1m_x1000(self):
        self._memCheckFunction(self._payloadEncodeDecode, 1000, 1 * 1024 ** 2)

    def testPayloadToFromEncodedPayload10mb(self):
        # Create Payload
        origPayload = makeTestPayloadA()
        origPayload.tuples = ["1234567890" * 1024 ** 2]

        encodedPayload = origPayload.toEncodedPayload()
        payload = Payload().fromEncodedPayload(encodedPayload)

        self.assertEqual(payload.tuples[0], origPayload.tuples[0])

    def testPayloadToFromEncodedPayload(self):
        # Create Payload
        origPayload = makeTestPayloadA()

        # To VortexServer Message
        encodedPayload = origPayload.toEncodedPayload()
        # print encodedPayload

        payload = Payload().fromEncodedPayload(encodedPayload)

        self.assertEqual(TupleHash(payload), TupleHash(payload))
        self.assertEqual(TupleHash(origPayload), TupleHash(origPayload))

        self.assertEqual(
            TupleHash(origPayload),
            TupleHash(payload),
            "Payload serialise -> deserialize error",
        )

    def testPayloadToFromJson(self):
        # Create Payload
        origPayload = makeTestPayloadA()

        # To JSON
        jsonStr = origPayload._toJson()
        # print encodedPayload
        # print(jsonStr)

        payload = Payload()._fromJson(jsonStr)

        self.assertEqual(TupleHash(payload), TupleHash(payload))
        self.assertEqual(TupleHash(origPayload), TupleHash(origPayload))

        self.assertEqual(
            TupleHash(origPayload),
            TupleHash(payload),
            "Payload serialise -> deserialise error",
        )

    def testPayloadJsonPerformanceSingle(self):
        origPayload = makeTestPayloadA()
        for _ in range(self.PERF_TEST_COUNT):
            origPayload.tuples.append(makeTestPayloadA())
        jsonStr = origPayload._toJson()
        payload = Payload()._fromJson(jsonStr)

    def testPayloadEncodedPayloadPerformanceSingle(self):
        origPayload = makeTestPayloadA()
        for x in range(self.PERF_TEST_COUNT):
            origPayload.tuples.append(makeTestPayloadA())
        encodedPayload = origPayload.toEncodedPayload()
        payload = Payload().fromEncodedPayload(encodedPayload)

    def testPayloadJsonPerformanceMultiple(self):
        origPayload = makeTestPayloadA()
        for _ in range(self.PERF_TEST_COUNT):
            jsonStr = origPayload._toJson()
            payload = Payload()._fromJson(jsonStr)

    def testPayloadEncodedPayloadPerformanceMultiple(self):
        origPayload = makeTestPayloadA()
        for _ in range(self.PERF_TEST_COUNT):
            encodedPayload = origPayload.toEncodedPayload()
            payload = Payload().fromEncodedPayload(encodedPayload)

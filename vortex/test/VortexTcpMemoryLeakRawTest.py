"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import tracemalloc

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.trial import unittest

from vortex.test.VortexTcpMemoryLeakPayloadTest import (
    MemoryCheckerTestMixin,
    VortexTcpConnectTestMixin,
)


class VortexTcpMemoryLeakRawTest(
    unittest.TestCase, MemoryCheckerTestMixin, VortexTcpConnectTestMixin
):
    # Increase the timeout to 15 minutes
    timeout = 30 * 60

    _patchedProcessData_dataReceivedLen = 0

    def _processData_patch(self):
        VortexTcpMemoryLeakRawTest._patchedProcessData_dataReceivedLen += len(
            self._data
        )

        # Just drop the data and continue
        self._data = b""

    @inlineCallbacks
    def test_1vortexSend50mb_oneway_msg1mb_client_drop_data(self):
        tracemalloc.start(5)
        yield self._connect()

        self._memMark()

        from vortex.VortexPayloadProtocol import VortexPayloadProtocol

        VortexPayloadProtocol._processData = (
            VortexTcpMemoryLeakRawTest._processData_patch
        )

        dataSent = 0
        for _ in range(1024):
            dataSent += 1024 ** 2
            self._vortexServer.sendVortexMsg(b"x" * 1024 ** 2)

        # while dataSent < self._patchedProcessData_dataReceivedLen:
        for _ in range(2):
            print(
                "Sleeping 2 seconds, send %s vs received %s"
                % (dataSent, self._patchedProcessData_dataReceivedLen)
            )

            d = Deferred()
            reactor.callLater(2, d.callback, True)
            yield d
            self._memPrintIncrease()

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics("lineno")

        print("[ Top 10 ]")
        for stat in top_stats[:10]:
            print(stat)

        tracemalloc.stop()

        # while dataSent < self._patchedProcessData_dataReceivedLen:
        for _ in range(2):
            print("Sleeping 2 seconds")

            d = Deferred()
            reactor.callLater(2, d.callback, True)
            yield d
            self._memPrintIncrease()

        self._memPrintIncrease()

        self._disconnect()

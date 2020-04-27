"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import gc
import os
from collections import deque
from random import random
from typing import Optional

import psutil
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.python.failure import Failure
from twisted.trial import unittest

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.VortexClientTcp import VortexClientTcp
from vortex.VortexServer import VortexServer
from vortex.VortexServerTcp import VortexTcpServerFactory


class MemoryCheckerTestMixin:

    def _memMark(self):
        self._process = psutil.Process(os.getpid())
        self._initialMem = self._process.memory_info().rss

    def _memCheck(self, grace=20 * 1024 * 1024):
        unreachable = gc.collect()
        memNow = self._process.memory_info().rss
        # self.assertFalse(unreachable, "The garbage collector couldn't release everything")
        self._memPrintIncrease()
        self.assertLessEqual(memNow, self._initialMem + grace)

    def _memPrintIncrease(self):
        unreachable = gc.collect()
        print("Memory growth is "
              + "{:,d}".format(self._process.memory_info().rss - self._initialMem))


class VortexTcpConnectTestMixin:

    @inlineCallbacks
    def _connect(self):
        port = 20000 + int(random() * 10000)

        # Create the server
        self._vortexServer = VortexServer('test server')
        vortexTcpServerFactory = VortexTcpServerFactory(self._vortexServer)
        self._listenPort = reactor.listenTCP(port, vortexTcpServerFactory)

        # Create the client
        self._vortexClient = VortexClientTcp('test client')
        yield self._vortexClient.connect('127.0.0.1', port)

    def _disconnect(self):
        self._listenPort.stopListening()
        del self._listenPort

        self._vortexServer.shutdown()
        del self._vortexServer

        self._vortexClient.close()
        del self._vortexClient


class VortexSendReceiveTestMixin:
    # Setup a queue state
    _loopbackFilt = {'key': 'unittest'}

    class _State:
        def __init__(self, testSelf):
            self.testSelf = testSelf

            self.dataSizeStats = []
            self.vortexSizeStats = []

            self.dataQueue = deque()
            self.dataQueueEmptyDeferred = Deferred()
            self.totalSent = 0
            self.totalReceived = 0
            self.totalMessagesSent = 0
            self.totalMessagesReceived = 0
            self.totalMessagesOutOfOrder = 0
            self.highestOutOfOrderIndex = 0

            self.totalSentFromClient = 0
            self.totalSentFromServer = 0

        def vortex(self, dataLen: int):
            if random() < 0.5:
                self.totalSentFromClient += dataLen
                return self.testSelf._vortexClient

            self.totalSentFromServer += dataLen
            return self.testSelf._vortexServer

    class _Checker:

        def __init__(self, testSelf, state, printStatusEveryXMessage):
            self.endpoint = None
            self.testSelf = testSelf
            self.state = state
            self.printStatusEveryXMessage = printStatusEveryXMessage

        def setEndpoint(self, endpoint):
            self.endpoint = endpoint

        # Setup the receiver of the data
        @inlineCallbacks
        def process(self, payloadEnvelope, *args, **kwargs):
            try:
                payload = yield payloadEnvelope.decodePayloadDefer()
                dataReceived = payload.tuples[0]
                self.state.totalReceived += len(dataReceived)
                self.state.totalMessagesReceived += 1

                dataSent = None
                for index, item in enumerate(self.state.dataQueue):
                    if item == dataReceived:
                        dataSent = dataReceived
                        if index:
                            self.state.totalMessagesOutOfOrder += 1
                            self.state.highestOutOfOrderIndex = \
                                max(self.state.highestOutOfOrderIndex, index)
                            self.state.dataQueue.remove(item)

                        else:
                            self.state.dataQueue.popleft()

                        break

                if not (self.state.totalMessagesReceived % self.printStatusEveryXMessage):
                    print("Received message %s, this %s, total %s"
                          % (self.state.totalMessagesReceived,
                             "{:,d}".format(len(dataReceived)),
                             "{:,d}".format(self.state.totalReceived)))

                self.testSelf.assertEqual(dataReceived, dataSent)

                if not self.state.dataQueue:
                    self.state.dataQueueEmptyDeferred.callback(True)

            except Exception as e:
                self.endpoint.shutdown()
                self.state.dataQueueEmptyDeferred.errback(Failure(e))

    @inlineCallbacks
    def _vortexTestTcpServerClient(self, printStatusEveryXMessage: int,
                                   maxMessageSizeBytes: int,
                                   totalBytesToSend: Optional[int] = None,
                                   totalMessagesToSend: Optional[int] = None,
                                   payloadCompression=9):
        assert totalBytesToSend or totalMessagesToSend, "We must have a total to send"

        state = self._State(self)
        checker = self._Checker(self, state, printStatusEveryXMessage)
        endpoint = PayloadEndpoint(self._loopbackFilt, checker.process)
        checker.setEndpoint(endpoint)

        # Make random chunks of data
        def makeData():
            size = random() * maxMessageSizeBytes
            packet = str(random())
            while len(packet) < size:
                packet += str(random())
            return packet

        def check():
            if totalMessagesToSend and state.totalMessagesSent < totalMessagesToSend:
                return True

            if totalBytesToSend and state.totalSent < totalBytesToSend:
                return True

            return False

        # Send the data
        while check():
            data = makeData()
            state.dataQueue.append(data)
            state.totalSent += len(data)
            state.totalMessagesSent += 1

            vortexMsg = yield Payload(self._loopbackFilt, data) \
                .makePayloadEnvelopeVortexMsgDefer(compressionLevel=payloadCompression)

            state.dataSizeStats.append(len(data))
            state.vortexSizeStats.append(len(vortexMsg))

            # We could send this from either the vortexClient or vortexServer
            # We only have one PayloadIO that the PayloadEndpoint binds to anyway
            state.vortex(len(data)).sendVortexMsg(vortexMsg)

            if not (state.totalMessagesSent % printStatusEveryXMessage):
                print("Sent     message %s, this %s, total %s"
                      % (state.totalMessagesSent,
                         "{:,d}".format(len(data)),
                         "{:,d}".format(state.totalSent)))

        # Wait for all the sending to complete
        yield state.dataQueueEmptyDeferred

        print("%s messages were out of order, the most out of order was %s"
              % (state.totalMessagesOutOfOrder, state.highestOutOfOrderIndex))

        print("Sent %s from the vortex client"
              % "{:,d}".format(state.totalSentFromClient))

        print("Sent %s from the vortex server"
              % "{:,d}".format(state.totalSentFromServer))

        print("Data     : count %s, total size %s, max size %s, min size %s, average %s"
              % (state.totalMessagesSent,
                 "{:,d}".format(state.totalSent),
                 "{:,d}".format(max(state.dataSizeStats)),
                 "{:,d}".format(min(state.dataSizeStats)),
                 "{:,d}".format(int(state.totalSent / state.totalMessagesSent))))

        totalVortexMsgs = sum(state.vortexSizeStats)
        print("VortexMsg: count %s, total size %s, max size %s, min size %s, average %s"
              % (state.totalMessagesSent,
                 "{:,d}".format(totalVortexMsgs),
                 "{:,d}".format(max(state.vortexSizeStats)),
                 "{:,d}".format(min(state.vortexSizeStats)),
                 "{:,d}".format(int(totalVortexMsgs / state.totalMessagesSent))))

        # Run our checks
        self.assertEqual(state.totalSent, state.totalReceived)
        self.assertFalse(len(state.dataQueue))

        # Cleanup
        endpoint.shutdown()
        del checker
        del state
        del endpoint

        gc.collect()


class VortexTcpConnectTest(unittest.TestCase,
                           MemoryCheckerTestMixin,
                           VortexTcpConnectTestMixin,
                           VortexSendReceiveTestMixin):
    @inlineCallbacks
    def __vortexReconnect(self, count):
        self._memMark()
        for x in range(count):
            print("Reconnecting #%s" % x)
            yield self._connect()
            self._disconnect()
            self._memPrintIncrease()
        self._memCheck()

    @inlineCallbacks
    def test_vortexReconnect100(self):
        yield self.__vortexReconnect(100)

    @inlineCallbacks
    def test_vortexReconnect500(self):
        yield self.__vortexReconnect(500)

    @inlineCallbacks
    def __vortexReconnect_with_data(self, count):
        self._memMark()
        for x in range(count):
            print("Reconnecting #%s" % x)
            yield self._connect()
            yield self._vortexTestTcpServerClient(totalMessagesToSend=10,
                                                  printStatusEveryXMessage=1,
                                                  maxMessageSizeBytes=100 * 1024)
            self._disconnect()
            self._memPrintIncrease()
        self._memCheck()

    @inlineCallbacks
    def test_vortexReconnect100_with_data(self):
        yield self.__vortexReconnect_with_data(100)

    @inlineCallbacks
    def test_vortexReconnect500_with_data(self):
        yield self.__vortexReconnect_with_data(500)


class VortexTcpMemoryLeakTest(unittest.TestCase,
                              MemoryCheckerTestMixin,
                              VortexTcpConnectTestMixin,
                              VortexSendReceiveTestMixin):

    @inlineCallbacks
    def __vortexSendMsgCountMsgSize(self, count, size, compression=9):
        self._memMark()
        yield self._connect()
        yield self._vortexTestTcpServerClient(totalMessagesToSend=count,
                                              printStatusEveryXMessage=1000,
                                              maxMessageSizeBytes=size,
                                              payloadCompression=compression)
        self._disconnect()
        self._memCheck()

    @inlineCallbacks
    def test_1vortexSend10000_1kb_compression1(self):
        yield self.__vortexSendMsgCountMsgSize(10000, 1 * 1024, 1)

    @inlineCallbacks
    def test_1vortexSend10000_100kb_compression1(self):
        yield self.__vortexSendMsgCountMsgSize(10000, 100 * 1024, 1)

    @inlineCallbacks
    def test_1vortexSend10000_1kb_compression9(self):
        yield self.__vortexSendMsgCountMsgSize(10000, 1 * 1024, 9)

    @inlineCallbacks
    def test_1vortexSend10000_100kb_compression9(self):
        yield self.__vortexSendMsgCountMsgSize(10000, 100 * 1024, 9)

    @inlineCallbacks
    def __vortexSendTotalSendMsgSize(self, totalSend, msgSize):
        self._memMark()
        yield self._connect()
        yield self._vortexTestTcpServerClient(totalBytesToSend=totalSend,
                                              printStatusEveryXMessage=1000,
                                              maxMessageSizeBytes=msgSize)
        self._disconnect()
        self._memCheck()

    @inlineCallbacks
    def test_2vortexSend1mb_1kb(self):
        yield self.__vortexSendTotalSendMsgSize(1024 ** 2, 1 * 1024)

    @inlineCallbacks
    def test_2vortexSend1mb_100kb(self):
        yield self.__vortexSendTotalSendMsgSize(1024 ** 2, 100 * 1024)

    @inlineCallbacks
    def test_3vortexSend1mb_X10_yes_reconnect(self):
        mem = MemoryCheckerTestMixin()
        mem._memMark()

        print(" ===== START LOOP ===== ")
        for i in range(10):
            print(" ===== Running iteration %s ===== " % i)
            self._memMark()
            yield self._connect()
            yield self._vortexTestTcpServerClient(totalBytesToSend=1024 ** 2,
                                                  printStatusEveryXMessage=1000,
                                                  maxMessageSizeBytes=100 * 1024)
            self._disconnect()
            self._memCheck()

        print(" ===== END LOOP ===== ")
        mem._memPrintIncrease()

    @inlineCallbacks
    def test_3vortexSend1mb_X10_no_reconnect(self):
        mem = MemoryCheckerTestMixin()
        mem._memMark()
        # Initialise the vortex
        yield self._connect()

        print(" ===== START LOOP ===== ")
        for i in range(10):
            print(" ===== Running iteration %s ===== " % i)
            # Setup the parameters for the test
            self._memMark()
            yield self._vortexTestTcpServerClient(totalBytesToSend=1024 ** 2,
                                                  printStatusEveryXMessage=1000,
                                                  maxMessageSizeBytes=1 * 1024)
            self._memCheck()

        self._disconnect()
        print(" ===== END LOOP ===== ")
        mem._memPrintIncrease()

    @inlineCallbacks
    def test_4vortexSend10mb(self):
        yield self.__vortexSendTotalSendMsgSize(1024 ** 2 * 10, 100 * 1024)

    @inlineCallbacks
    def test_5vortexSend100mb(self):
        yield self.__vortexSendTotalSendMsgSize(1024 ** 2 * 100, 1000 * 1024)

    @inlineCallbacks
    def test_6vortexSend500mb(self):
        yield self.__vortexSendTotalSendMsgSize(1024 ** 2 * 500, 1000 * 1024)


class VortexTcpMemoryLeakLongTest(unittest.TestCase,
                                  MemoryCheckerTestMixin,
                                  VortexTcpConnectTestMixin,
                                  VortexSendReceiveTestMixin):
    # Increase the timeout to 15 minutes
    timeout = 30 * 60

    @inlineCallbacks
    def __vortexSendTest(self, total, compression):
        self._memMark()
        yield self._connect()
        yield self._vortexTestTcpServerClient(totalBytesToSend=total,
                                              printStatusEveryXMessage=10000,
                                              maxMessageSizeBytes=1000 * 1024,
                                              payloadCompression=compression)
        self._disconnect()
        self._memCheck()

    @inlineCallbacks
    def test_vortexSend1gb_compression1(self):
        yield self.__vortexSendTest(1024 ** 3, compression=1)

    @inlineCallbacks
    def test_vortexSend1gb_compression9(self):
        yield self.__vortexSendTest(1024 ** 3, compression=9)

    @inlineCallbacks
    def test_vortexSend3gb_compression1(self):
        yield self.__vortexSendTest(1024 ** 3 * 3, compression=1)

    @inlineCallbacks
    def test_vortexSend3gb_compression9(self):
        yield self.__vortexSendTest(1024 ** 3 * 3, compression=9)

    @inlineCallbacks
    def test_vortexSend10gb_compression1(self):
        yield self.__vortexSendTest(1024 ** 3 * 10, compression=1)

    @inlineCallbacks
    def test_vortexSend10gb_compression9(self):
        yield self.__vortexSendTest(1024 ** 3 * 10, compression=9)

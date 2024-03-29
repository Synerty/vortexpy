"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
import typing
import weakref
from collections import deque, defaultdict
from typing import Callable, Deque, Dict

from twisted.internet.interfaces import IPushProducer
from zope.interface import implementer

from vortex.DeferUtil import nonConcurrentMethod
from vortex.PayloadPriority import DEFAULT_PRIORITY

logger = logging.getLogger(name=__name__)


def _format_size(size):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 100 and unit != "B":
            # 3 digits (xx.x UNIT)
            return "%.1f %s" % (size, unit)
        if abs(size) < 10 * 1024 or unit == "TiB":
            # 4 or 5 digits (xxxx UNIT)
            return "%.0f %s" % (size, unit)
        size /= 1024


@implementer(IPushProducer)
class VortexWritePushProducer:
    WARNING_DATA_LENGTH = 50 * 1024 * 1024
    ERROR_DATA_LENGTH = 500 * 1024 * 1024
    WRITE_CHUNK_SIZE = 128 * 1024

    __memoryLoggingRefs = None
    __memoryLoggingEnabled = False
    _DEBUG_LOGGING = False

    @classmethod
    def setupMemoryLogging(cls) -> None:
        cls.__memoryLoggingRefs = []
        cls.__memoryLoggingEnabled = True

    @classmethod
    def memoryLoggingDump(
        cls, top=10, msgs=1
    ) -> typing.List[typing.Tuple[str, int, int]]:
        if not cls.__memoryLoggingRefs:
            return []

        # Filter out expired items
        cls.__memoryLoggingRefs = list(
            filter(lambda o: o(), cls.__memoryLoggingRefs)
        )

        results = []
        for producerRef in cls.__memoryLoggingRefs:
            producer = producerRef()
            if not producer:
                continue

            queueCount = sum(
                [len(q) for q in producer._queueByPriority.values()]
            )

            results.append(
                (
                    producer._remoteVortexName,
                    queueCount,
                    producer._queuedDataLen,
                )
            )

        data = sorted(results, key=lambda x: x[2], reverse=True)

        return list(filter(lambda x: x[2] >= msgs, data))[:top]

    def __init__(
        self,
        transport,
        stopProducingCallback: Callable,
        remoteVortexName: str = "Pending",
        writeWholeFrames=False,
        terminateFrameWithDot=True,
        splitFrames=False,
    ):
        """

        :param splitFrames: Split frames that come in if they are larger than 128kb.
        This allows better use of ram, as keeping a 1mb to ?mb bytes object around
        is bad for memory management and memory fragmentation.
        """
        if VortexWritePushProducer.__memoryLoggingEnabled:
            VortexWritePushProducer.__memoryLoggingRefs.append(
                weakref.ref(self)
            )

        self._transport = transport
        self._stopProducingCallback = stopProducingCallback
        self._remoteVortexName = remoteVortexName
        self._writeWholeFrames = writeWholeFrames
        self._terminateFrameWithDot = terminateFrameWithDot
        self._splitFrames = splitFrames

        self._paused = False
        self._pausedDeferred = None
        self._writingInProgress = False
        self._writingFrameInProgress = False
        self._closed = False
        self._queuedDataLen = 0
        self._queueByPriority: Dict[int, Deque] = defaultdict(deque)

        self._currentlyWritingData: typing.Optional[bytes] = None
        self._currentlyWritingDataOffset: int = 0

    def setRemoteVortexName(self, remoteVortexName: str):
        self._remoteVortexName = remoteVortexName

    @property
    def _canContinue(self):
        return (
            not self._paused
            and not self._startWritingFrame.running
            and [q for q in self._queueByPriority.values() if q]
        )

    @nonConcurrentMethod
    def _startWriting(self):
        if self._currentlyWritingData is not None:
            self._startWritingFrame()

        while self._canContinue:
            # Send the messages in order of priority
            for priority in sorted(self._queueByPriority):
                if not self._canContinue:
                    return

                queue = self._queueByPriority[priority]
                if not queue:
                    continue

                data = queue.popleft()
                preLen = self._queuedDataLen
                self._queuedDataLen -= len(data)

                if self._queuedDataLen < self.WARNING_DATA_LENGTH < preLen:
                    logger.info(
                        "%s: Data Queue memory high warning - returned to normal : %s",
                        self._remoteVortexName,
                        _format_size(self._queuedDataLen),
                    )

                if self._queuedDataLen < self.ERROR_DATA_LENGTH < preLen:
                    logger.info(
                        "%s: Data Queue memory high error - returned to warning : %s",
                        self._remoteVortexName,
                        _format_size(self._queuedDataLen),
                    )

                if self._writeWholeFrames:
                    self._transport.write(data)
                    if self._terminateFrameWithDot:
                        self._transport.write(b".")

                else:
                    # Use a memoryview for fast slicing.
                    self._currentlyWritingData = data
                    self._currentlyWritingDataOffset = 0
                    self._startWritingFrame()

    @nonConcurrentMethod
    def _startWritingFrame(self):
        # ---------------
        # Write in progress logic.
        # We should only have one write loop at a time
        if self._writingFrameInProgress:
            return
        self._writingFrameInProgress = True

        data = self._currentlyWritingData
        offset = self._currentlyWritingDataOffset
        while not self._paused and offset < len(data):
            if self._DEBUG_LOGGING:
                logger.debug(
                    "%s: Producer _startWritingFrame called,"
                    " wrote %s to %s of %s",
                    self._remoteVortexName,
                    offset,
                    min(offset + self.WRITE_CHUNK_SIZE, len(data)),
                    len(data),
                )
            self._transport.write(data[offset : offset + self.WRITE_CHUNK_SIZE])
            offset += self.WRITE_CHUNK_SIZE

        if len(data) <= offset:
            self._currentlyWritingDataOffset = 0
            self._currentlyWritingData = None
            if self._terminateFrameWithDot:
                self._transport.write(b".")

        else:
            self._currentlyWritingDataOffset = offset

        self._writingFrameInProgress = False

    def pauseProducing(self):
        """
        Pause producing data.

        Tells a producer that it has produced too much data to process for
        the time being, and to stop until C{resumeProducing()} is called.
        """
        self._paused = True
        if self._DEBUG_LOGGING:
            logger.debug(
                "%s: Producer paused, data len = %s",
                self._remoteVortexName,
                self._queuedDataLen,
            )

    def resumeProducing(self):
        """
        Resume producing data.

        This tells a producer to re-add itself to the main loop and produce
        more data for its consumer.
        """
        self._paused = False
        if self._DEBUG_LOGGING:
            logger.debug(
                "%s: Producer resumed, data len = %s",
                self._remoteVortexName,
                self._queuedDataLen,
            )
        self._startWriting()

    def stopProducing(self):
        """
        Stop producing data.

        This tells a producer that its consumer has died, so it must stop
        producing data for good.
        """
        if self._stopProducingCallback:
            self._stopProducingCallback()
            self._stopProducingCallback = None

    def write(self, data: bytes, priority: int = DEFAULT_PRIORITY):
        assert not self._closed

        # Since write actually queues data and does not write, we will split
        # the data if required so we are storing smaller memory objects.
        if self._splitFrames and self.WRITE_CHUNK_SIZE < len(data):
            for offset in range(0, len(data), self.WRITE_CHUNK_SIZE):

                if self._DEBUG_LOGGING:
                    logger.debug(
                        "%s: Producer write, splitting frames,"
                        " split %s to %s of %s",
                        self._remoteVortexName,
                        offset,
                        min(offset + self.WRITE_CHUNK_SIZE, len(data)),
                        len(data),
                    )
                self.write(data[offset : offset + self.WRITE_CHUNK_SIZE])
            return

        if self._DEBUG_LOGGING:
            logger.debug(
                "%s: Producer write called, data len = %s",
                self._remoteVortexName,
                len(data),
            )

        preLen = self._queuedDataLen
        self._queuedDataLen += len(data)

        # Queue the data up in chunks
        self._queueByPriority[priority].append(data)

        if preLen < self.WARNING_DATA_LENGTH < self._queuedDataLen:
            logger.warning(
                "%s: Data Queue memory high warning : %s",
                self._remoteVortexName,
                _format_size(self._queuedDataLen),
            )

        if preLen < self.ERROR_DATA_LENGTH < self._queuedDataLen:
            logger.error(
                "%s: Data Queue memory high error : %s",
                self._remoteVortexName,
                _format_size(self._queuedDataLen),
            )

        self._startWriting()

    def writeSequence(
        self, data: typing.Iterable[bytes], priority: int = DEFAULT_PRIORITY
    ) -> None:

        if self._DEBUG_LOGGING:
            logger.debug(
                "%s: Producer writeSequence called",
                self._remoteVortexName,
            )

        for seq in data:
            self.write(seq, priority=priority)

    def close(self):
        self._closed = True
        self._queuedDataLen = 0
        self._queueByPriority = defaultdict(deque)
        self._currentlyWritingData = None
        self._currentlyWritingDataOffset = 0

"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from collections import deque, defaultdict
from typing import Callable, Deque, Dict

from twisted.internet.interfaces import IPushProducer
from twisted.internet.task import coiterate
from zope.interface import implementer

logger = logging.getLogger(name=__name__)


def _format_size(size):
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if abs(size) < 100 and unit != 'B':
            # 3 digits (xx.x UNIT)
            return "%.1f %s" % (size, unit)
        if abs(size) < 10 * 1024 or unit == 'TiB':
            # 4 or 5 digits (xxxx UNIT)
            return "%.0f %s" % (size, unit)
        size /= 1024


@implementer(IPushProducer)
class VortexWritePushProducer(object):
    WARNING_DATA_LENGTH = 50 * 1024 * 1024
    ERROR_DATA_LENGTH = 50 * 1024 * 1024

    def __init__(self, transport,
                 stopProducingCallback: Callable,
                 remoteVortexName: str = 'Pending'):
        self._transport = transport
        self._stopProducingCallback = stopProducingCallback
        self._remoteVortexName = remoteVortexName

        self._paused = False
        self._writingInProgress = False
        self._closed = False
        self._queuedDataLen = 0
        self._queueByPriority: Dict[int, Deque] = defaultdict(deque)

    def setRemoteVortexName(self, remoteVortexName: str):
        self._remoteVortexName = remoteVortexName

    def _startWriting(self):
        coiterate(self._writeLoop())

    def _writeLoop(self):

        # ---------------
        # Write in progress logic.
        # We should only have one write loop at a time
        if self._writingInProgress:
            return
        self._writingInProgress = True

        # Send the messages in order of priority
        for priority in sorted(self._queueByPriority):
            queue = self._queueByPriority[priority]

            while self._queueByPriority and not self._paused:
                data = queue.popleft()
                if not queue:
                    del self._queueByPriority[priority]

                preLen = self._queuedDataLen
                self._queuedDataLen -= len(data)

                if self._queuedDataLen < self.WARNING_DATA_LENGTH < preLen:
                    logger.info(
                        "%s: Data Queue memory high warning - returned to normal : %s",
                        self._remoteVortexName,
                        _format_size(self._queuedDataLen))

                if self._queuedDataLen < self.ERROR_DATA_LENGTH < preLen:
                    logger.info(
                        "%s: Data Queue memory high error - returned to warning : %s",
                        self._remoteVortexName,
                        _format_size(self._queuedDataLen))

                logger.debug("%s: Producer paused, data len = %s",
                             self._remoteVortexName, self._queuedDataLen)
                self._transport.write(data, )

                yield None  # Let the reactor have some time

        self._writingInProgress = False

    def pauseProducing(self):
        """
        Pause producing data.

        Tells a producer that it has produced too much data to process for
        the time being, and to stop until C{resumeProducing()} is called.
        """
        self._paused = True
        # logger.debug("%s: Producer paused, data len = %s",
        #              self._remoteVortexName, self._queuedDataLen)

    def resumeProducing(self):
        """
        Resume producing data.

        This tells a producer to re-add itself to the main loop and produce
        more data for its consumer.
        """
        self._paused = False
        # logger.debug("%s: Producer resumed, data len = %s",
        #              self._remoteVortexName, self._queuedDataLen)
        self._startWriting()

    def stopProducing(self):
        """
        Stop producing data.

        This tells a producer that its consumer has died, so it must stop
        producing data for good.
        """
        self._stopProducingCallback()

    def write(self, data, priority: int):
        assert not self._closed

        preLen = self._queuedDataLen
        self._queuedDataLen += len(data)

        # Queue the data up in chunks
        self._queueByPriority[priority].append(data)

        if preLen < self.WARNING_DATA_LENGTH < self._queuedDataLen:
            logger.warning("%s: Data Queue memory high warning : %s",
                           self._remoteVortexName,
                           _format_size(self._queuedDataLen))

        if preLen < self.ERROR_DATA_LENGTH < self._queuedDataLen:
            logger.error("%s: Data Queue memory high error : %s",
                         self._remoteVortexName,
                         _format_size(self._queuedDataLen))

        self._startWriting()

    def close(self):
        self._closed = True

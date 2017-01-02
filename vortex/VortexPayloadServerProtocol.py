"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging

from .PayloadIO import PayloadIO
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import Protocol

from .Payload import Payload

logger = logging.getLogger(name=__name__)


class VortexPayloadServerProtocol(Protocol):
    def __init__(self):
        self._data = ""

    @inlineCallbacks
    def _processData(self):
        if not self._data:
            return

        def getNextChunk():
            try:
                return self._data.index('.')
            except ValueError:
                # There is no '.' in it, wait for more data.
                return None

        nextChunk = getNextChunk()

        while nextChunk:
            vortexMsg = self._data[:nextChunk]
            self._data = self._data[nextChunk + 1:]

            try:
                payload = yield Payload().fromVortexMsgDefer(vortexMsg)

                if payload.isEmpty():
                    return

                PayloadIO().process(payload, vortexUuid=self._vortexUuid)

            except Exception as e:
                self._logger.exception(e)
                raise

            nextChunk = getNextChunk()

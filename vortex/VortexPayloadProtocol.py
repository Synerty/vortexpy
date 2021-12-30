"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from abc import ABCMeta, abstractmethod
from collections import deque

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionDone, ConnectionLost
from twisted.internet.protocol import Protocol, connectionDone

from vortex.DeferUtil import nonConcurrentMethod
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadIO import PayloadIO

logger = logging.getLogger(name=__name__)


class VortexPayloadProtocol(Protocol, metaclass=ABCMeta):
    def __init__(self, logger):
        self._data = b""
        self._serverVortexUuid = None
        self._serverVortexName = None
        self._logger = logger

        self._processVortexMsgsInProgress = False
        self._vortexMsgsQueue = deque()

    @abstractmethod
    def _beat(self):
        """
        EG :
        if self._vortexClient:
            self._vortexClient._beat()
        """

    @abstractmethod
    def _nameAndUuidReceived(self, name, uuid):
        """
        EG :
        if self._vortexClient:
            self._vortexClient._setNameAndUuid(name=self._serverVortexName,
                                               uuid=self._serverVortexUuid)
        """

    @abstractmethod
    def _createResponseSenderCallable(self):
        """
        EG :
        def sendResponse(vortexMsgs: Union[VortexMsgList, bytes]):
            return self._vortexClient.sendVortexMsg(vortexMsgs=vortexMsgs)
        """

    def dataReceived(self, bytesIn):
        if bytesIn.startswith(b"<"):
            raise Exception("Not Logged In")

        self._data += bytesIn
        self._beat()
        self._processData()

    def connectionLost(self, reason=connectionDone):
        reasonFailure = reason
        self._processData()
        if reasonFailure.check(ConnectionDone):
            self._logger.info(
                "Connection closed by other end (it may be shutting down)"
            )

        elif isinstance(reasonFailure.value, ConnectionLost):
            self._logger.info(
                "Connection to other end lost (We may be shutting down)"
            )

        else:
            self._logger.error("Closed with error")
            try:
                self._logger.exception(reason.getErrorMessage())
                self._logger.exception(reason.getTraceback())
            except:
                self._logger.exception(reasonFailure.value)

    def _processData(self):
        if not self._data:
            return

        def getNextChunkIter():
            try:
                while True:
                    yield self._data.index(b".")
            except ValueError:
                # There is no '.' in it, wait for more data.
                return

        for nextChunk in getNextChunkIter():
            vortexMsg = self._data[:nextChunk]
            self._data = self._data[nextChunk + 1 :]

            # If we get two heartbeats in a row, this will be false
            if len(vortexMsg):
                self._vortexMsgsQueue.append(vortexMsg)

        if self._vortexMsgsQueue and not self._processVortexMsgs.running:
            reactor.callLater(0, self._processVortexMsgs)

    @inlineCallbacks
    @nonConcurrentMethod
    def _processVortexMsgs(self):
        while self._vortexMsgsQueue:
            vortexMsg = self._vortexMsgsQueue.popleft()

            if b"." in vortexMsg:
                raise Exception(
                    "Something went wrong, there is a '.' in the msg"
                )

            try:
                payloadEnvelope = yield PayloadEnvelope().fromVortexMsgDefer(
                    vortexMsg
                )

                if payloadEnvelope.isEmpty():
                    self._processServerInfoPayload(payloadEnvelope)
                else:
                    self._deliverPayload(payloadEnvelope)

            except Exception as e:
                print(vortexMsg)
                print(e)
                self._logger.exception(e)
                raise

    def _processServerInfoPayload(self, payload):
        """Process Server Info Payload

        The first payload a server sends to the client contains information about it's
        self.

        """

        if PayloadEnvelope.vortexUuidKey in payload.filt:
            self._serverVortexUuid = payload.filt[PayloadEnvelope.vortexUuidKey]

        if PayloadEnvelope.vortexNameKey in payload.filt:
            self._serverVortexName = payload.filt[PayloadEnvelope.vortexNameKey]

        self._nameAndUuidReceived(
            name=self._serverVortexName, uuid=self._serverVortexUuid
        )

    def _deliverPayload(self, payload):

        PayloadIO().process(
            payload,
            vortexUuid=self._serverVortexUuid,
            vortexName=self._serverVortexName,
            httpSession=None,
            sendResponse=self._createResponseSenderCallable(),
        )

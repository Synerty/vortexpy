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
from typing import Union

from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionDone, ConnectionLost
from twisted.internet.protocol import Protocol, connectionDone
from twisted.web._newclient import ResponseDone, ResponseNeverReceived, ResponseFailed
from twisted.web.http import _DataLoss

from vortex.Payload import Payload, VortexMsgList
from vortex.PayloadIO import PayloadIO

logger = logging.getLogger(name=__name__)


class VortexPayloadProtocol(Protocol, metaclass=ABCMeta):
    def __init__(self, logger):
        self._data = b""
        self._serverVortexUuid = None
        self._serverVortexName = None
        self._logger = logger

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

    def dataReceived(self, bytes):
        if bytes.startswith(b"<"):
            raise Exception("Not Logged In")

        self._data += bytes
        self._beat()
        self._processData()

    def connectionLost(self, reason=connectionDone):
        reasonFailure = reason
        self._processData()
        if (reasonFailure.check(ConnectionDone)):
            self._logger.info("Connection closed by other end (it may be shutting down)")

        elif isinstance(reasonFailure.value, ConnectionLost):
            self._logger.info("Connection to other end lost (We may be shutting down)")

        else:
            self._logger.error("Closed with error")
            try:
                self._logger.exception(reason.getErrorMessage())
                self._logger.exception(reason.getTraceback())
            except:
                self._logger.exception(reasonFailure.value)

    @inlineCallbacks
    def _processData(self):
        if not self._data:
            return

        def getNextChunk():
            try:
                return self._data.index(b'.')
            except ValueError:
                # There is no '.' in it, wait for more data.
                return None

        while getNextChunk() is not None:  # NOT ZERO
            nextChunk = getNextChunk()
            vortexMsg = self._data[:nextChunk]

            if b"." in vortexMsg:
                print(vortexMsg)
                raise Exception("Something went wrong, there is a '.' in the msg")

            self._data = self._data[nextChunk + 1:]

            self._beat()

            # If the vortex message is omitted entirly, then this is just a heart beat.
            if len(vortexMsg) == 0:
                continue

            try:
                payload = yield Payload().fromVortexMsgDefer(vortexMsg)

                if payload.isEmpty():
                    self._processServerInfoPayload(payload)
                else:
                    self._deliverPayload(payload)

            except Exception as e:
                print(vortexMsg)
                print(e)
                self._logger.exception(e)
                raise

    def _processServerInfoPayload(self, payload):
        """ Process Server Info Payload

        The first payload a server sends to the client contains information about it's
        self.

        """

        if Payload.vortexUuidKey in payload.filt:
            self._serverVortexUuid = payload.filt[Payload.vortexUuidKey]

        if Payload.vortexNameKey in payload.filt:
            self._serverVortexName = payload.filt[Payload.vortexNameKey]

        self._nameAndUuidReceived(name=self._serverVortexName,
                                  uuid=self._serverVortexUuid)



    def _deliverPayload(self, payload):

        PayloadIO().process(payload,
                            vortexUuid=self._serverVortexUuid,
                            vortexName=self._serverVortexName,
                            httpSession=None,
                            sendResponse=self._createResponseSenderCallable()
                            )

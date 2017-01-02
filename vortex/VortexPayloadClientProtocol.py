"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
from typing import Union

from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionDone, ConnectionLost
from twisted.internet.protocol import Protocol, connectionDone
from twisted.web._newclient import ResponseDone, ResponseNeverReceived, ResponseFailed
from twisted.web.http import _DataLoss

from vortex.Payload import Payload, VortexMsgList
from vortex.PayloadIO import PayloadIO


class VortexPayloadClientProtocol(Protocol):
    def __init__(self, logger, vortexClient=None):
        self._vortexClient = vortexClient
        self._data = b""
        self._serverVortexUuid = None
        self._serverVortexName = None
        self._logger = logger

    def _beat(self):
        if self._vortexClient:
            self._vortexClient._beat()

    @property
    def serverVortexUuid(self):
        return self._serverVortexUuid

    @property
    def serverVortexName(self):
        return self._serverVortexName

    def dataReceived(self, bytes):
        if bytes.startswith(b"<"):
            raise Exception("Not Logged In")

        self._data += bytes
        self._beat()
        self._processData()

    def connectionLost(self, reason=connectionDone):
        reasonFailure = reason
        self._processData()
        if isinstance(reasonFailure.value, ResponseDone):
            self._logger.debug("Closed cleanly by server")

        elif isinstance(reasonFailure.value, ResponseNeverReceived):
            self._logger.debug("Server didn't answer")

        elif (isinstance(reasonFailure.value, ResponseFailed)
              and len(reasonFailure.value.reasons) == 2
              and isinstance(reasonFailure.value.reasons[0].value, ConnectionDone)
              and isinstance(reasonFailure.value.reasons[1].value, _DataLoss)):
            self._logger.info("Connection closed by server (Server may be shutting down)")

        elif (isinstance(reasonFailure.value, ResponseFailed)
              and len(reasonFailure.value.reasons) == 2
              and isinstance(reasonFailure.value.reasons[0].value, ConnectionLost)
              and isinstance(reasonFailure.value.reasons[1].value, _DataLoss)):
            self._logger.info("Connection to server lost (We may be shutting down)")

        else:
            self._logger.error("Closed with error")
            try:
                for reason in reasonFailure.value.reasons:
                    self._logger.exception(reason)
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

        nextChunk = getNextChunk()

        while nextChunk:
            vortexMsg = self._data[:nextChunk]
            self._data = self._data[nextChunk + 1:]

            try:
                payload = yield Payload().fromVortexMsgDefer(vortexMsg)

                if payload.isEmpty():
                    if Payload.vortexUuidKey in payload.filt:
                        self._serverVortexUuid = payload.filt[Payload.vortexUuidKey]

                    if Payload.vortexNameKey in payload.filt:
                        self._serverVortexName = payload.filt[Payload.vortexNameKey]

                    self._beat()
                    return

                def sendResponse(vortexMsgs: Union[VortexMsgList, bytes]):
                    """ Send Response

                    Sends a response back to where this payload come from.

                    """
                    return self._vortexClient.sendVortexMsg(vortexMsgs=vortexMsgs)

                PayloadIO().process(payload,
                                    vortexUuid=self._serverVortexUuid,
                                    vortexName=self._serverVortexName,
                                    httpSession=None,
                                    sendResponse=sendResponse
                                    )

            except Exception as e:
                self._logger.exception(e)
                raise

            nextChunk = getNextChunk()

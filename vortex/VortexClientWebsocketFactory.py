"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
import uuid
from collections import deque, namedtuple
from datetime import datetime
from typing import Union, Optional, List, Deque
from urllib.parse import urlencode, urlparse, urlunparse

import pytz
import twisted
from autobahn.twisted.websocket import (
    WebSocketClientProtocol,
    WebSocketClientFactory,
    connectWS,
)
from twisted.internet import reactor
from twisted.internet import task
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.protocol import connectionDone, ReconnectingClientFactory

from vortex.DeferUtil import isMainThread, vortexLogFailure, nonConcurrentMethod
from vortex.PayloadEnvelope import PayloadEnvelope, VortexMsgList
from vortex.PayloadPriority import DEFAULT_PRIORITY
from vortex.VortexABC import VortexABC, VortexInfo
from vortex.VortexPayloadProtocol import VortexPayloadProtocol
from vortex.VortexServer import HEART_BEAT_PERIOD
from vortex.VortexServer import HEART_BEAT_TIMEOUT
from vortex.VortexUtil import logLargeMessages

logger = logging.getLogger(name=__name__)

VortexMsgToSend = namedtuple("VortexMsgToSend", ["deferred", "vortexMsgs"])


class VortexPayloadWebsocketClientProtocol(
    WebSocketClientProtocol, VortexPayloadProtocol
):
    def __init__(self, vortexClient: "VortexClientWebsocketFactory"):
        WebSocketClientProtocol.__init__(self)
        VortexPayloadProtocol.__init__(self, logger)
        self._vortexClient = vortexClient

        self._vortexClientConnectionMade()

    def _vortexClientConnectionMade(self):
        self._closed = False

        # Start our heart beat
        self._sendBeatLoopingCall = task.LoopingCall(self._sendBeat)
        d = self._sendBeatLoopingCall.start(HEART_BEAT_PERIOD, now=False)
        d.addErrback(lambda f: logger.exception(f.value))

    def onMessage(self, message, isBinary):
        VortexPayloadProtocol.vortexMsgReceived(self, message)

    def _beat(self):
        if self._vortexClient:
            self._vortexClient._beat()

    def _nameAndUuidReceived(self, name, uuid):
        from vortex.VortexFactory import VortexFactory

        # self._producer.setRemoteVortexName(name)

        if self._vortexClient:
            self._vortexClient._setNameAndUuid(name=name, uuid=uuid)

        VortexFactory._notifyOfVortexStatusChange(name, online=True)

    def _createResponseSenderCallable(self):
        def sendResponse(
            vortexMsgs: Union[VortexMsgList, bytes],
            priority: int = DEFAULT_PRIORITY,
        ):
            return self._vortexClient.sendVortexMsg(
                vortexMsgs=vortexMsgs, priority=priority
            )

        return sendResponse

    def _sendBeat(self):
        if self._closed:
            return

        # Send the heartbeats
        self.sendMessage(b".")

    def write(self, payloadVortexStr: bytes, _priority: int = DEFAULT_PRIORITY):
        if not twisted.python.threadable.isInIOThread():
            e = Exception("Write called from NON main thread")
            logger.exception(str(e))
            raise e

        assert not self._closed
        self.sendMessage(payloadVortexStr)

    def onConnect(self, response):
        logger.info(f"Connected to {response.peer}")

        self._vortexClientConnectionMade()

        # Send a heart beat down the new connection, tell it who we are.
        connectPayloadFilt = {
            PayloadEnvelope.vortexUuidKey: self._vortexClient.uuid,
            PayloadEnvelope.vortexNameKey: self._vortexClient.name,
        }
        self.write(PayloadEnvelope(filt=connectPayloadFilt).toVortexMsg())

    def connectionLost(self, reason=connectionDone):
        from vortex.VortexFactory import VortexFactory

        VortexFactory._notifyOfVortexStatusChange(
            self._serverVortexName, online=False
        )

        if self._sendBeatLoopingCall.running:
            self._sendBeatLoopingCall.stop()
        self._closed = True

    def close(self):
        self.transport.loseConnection()
        if self._sendBeatLoopingCall.running:
            self._sendBeatLoopingCall.stop()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed


class VortexClientWebsocketFactory(
    WebSocketClientFactory, ReconnectingClientFactory, VortexABC
):
    maxDelay = 1.0

    def __init__(self, name: str, *args, **kwargs):

        ReconnectingClientFactory.__init__(self)

        self._vortexName = name
        self._vortexUuid = str(uuid.uuid4())

        if "url" in kwargs:
            params = dict(
                vortexName=self._vortexName, vortexUuid=self._vortexUuid
            )

            url_parts = list(urlparse(kwargs["url"]))
            query = dict(url_parts[4])
            query.update(params)

            url_parts[4] = urlencode(query)
            url = urlunparse(url_parts)
            kwargs["url"] = url

        WebSocketClientFactory.__init__(self, *args, **kwargs)
        self.setProtocolOptions(utf8validateIncoming=False)

        self._server = None
        self._port = None

        self._retrying = False

        self._serverVortexName = None
        self._serverVortexUuid = None

        self._lastBeatReceiveTime = None
        self._lastHeartBeatCheckTime = datetime.now(pytz.utc)

        # Start our heart beat checker
        self._beatLoopingCall = task.LoopingCall(self._checkBeat)

        self._reconnectVortexMsgs = [PayloadEnvelope().toVortexMsg()]

        # Buffer messages while the reconnecting factory is reconnecting
        # but only in that state, if the vortex times out, we errback alll these
        self._pendingMessages: Deque[VortexMsgToSend] = deque()

        self.__protocol = None

    @property
    def name(self):
        return self._vortexName

    @property
    def uuid(self):
        return self._vortexUuid

    @property
    def requiresBase64Encoding(self) -> bool:
        return False

    @property
    def localVortexInfo(self) -> VortexInfo:
        return VortexInfo(name=self._vortexName, uuid=self._vortexUuid)

    @property
    def remoteVortexInfo(self) -> List[VortexInfo]:
        if not self.__protocol:
            return []

        if not self._serverVortexUuid:
            return []

        return [
            VortexInfo(name=self._serverVortexName, uuid=self._serverVortexUuid)
        ]

    def buildProtocol(self, addr) -> VortexPayloadWebsocketClientProtocol:
        logger.debug(f"Building protocol for {addr}")
        self._close()

        # Reset the times in the ReconnectingClientFactory
        self.resetDelay()

        self.__protocol = VortexPayloadWebsocketClientProtocol(self)
        self.__protocol.factory = self
        return self.__protocol

    def clientConnectionLost(self, connector, reason):
        logger.debug("Lost connection, Reason: %s", reason.value)
        logger.info("Lost connection, Trying to reconnect.")
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        logger.debug("Connection failed. Reason: %s", reason.value)
        logger.info("Connection failed, Trying to reconnect.")
        ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason
        )

    @inlineCallbacks
    def connect(self, server, port, sslContextFactory):
        self._server = server
        self._port = port
        self._sslContextFactory = sslContextFactory

        self._beat()
        d = self._beatLoopingCall.start(HEART_BEAT_PERIOD, now=False)
        d.addErrback(vortexLogFailure, logger, consumeError=True)
        deferred = Deferred()

        yield connectWS(self, sslContextFactory)

        def checkUuid():
            if self._serverVortexName:
                deferred.callback(True)
            else:
                reactor.callLater(0.1, checkUuid)

        checkUuid()

        return (yield deferred)

    def close(self):
        if self._beatLoopingCall and self._beatLoopingCall.running:
            self._beatLoopingCall.stop()
            self._beatLoopingCall = None

        # Stop the ReconnectingClientFactory from trying to reconnect
        self.stopTrying()
        self._close()

    def addReconnectVortexMsg(self, vortexMsg: bytes):
        """Add Reconnect Payload

        :param vortexMsg: An encoded PayloadEnvelope to send when the connection reconnects
        :return:
        """
        self._reconnectVortexMsgs.append(vortexMsg)

    def sendVortexMsg(
        self,
        vortexMsgs: Union[VortexMsgList, bytes, None] = None,
        vortexUuid: Optional[str] = None,
        priority: int = DEFAULT_PRIORITY,
    ) -> Deferred:
        """Send Vortex Msg

        NOTE: Priority ins't supported as there is no buffer for this class.

        """
        if not self.__protocol:
            from vortex.VortexFactory import NoVortexException

            raise NoVortexException("The vortex is not yet connected")

        if vortexMsgs is None:
            vortexMsgs = self._reconnectVortexMsgs

        if not isinstance(vortexMsgs, list):
            vortexMsgs = [vortexMsgs]

        logLargeMessages(logger, vortexMsgs, self._serverVortexUuid)

        if isMainThread():
            return self._sendVortexMsgLater(vortexMsgs)

        return task.deferLater(reactor, 0, self._sendVortexMsgLater, vortexMsgs)

    @inlineCallbacks
    def _sendVortexMsgLater(self, vortexMsgs: VortexMsgList):
        yield None

        if not self.__protocol:
            from vortex.VortexFactory import NoVortexException

            raise NoVortexException("The vortex is not yet connected")

        assert self._server
        assert vortexMsgs

        # This transport requires base64 encoding
        for index, vortexMsg in enumerate(vortexMsgs):
            if vortexMsg.startswith(b"{"):
                vortexMsgs[index] = yield PayloadEnvelope.base64EncodeDefer(
                    vortexMsg
                )

        for vortexMsg in vortexMsgs:
            self.__protocol.write(vortexMsg)

        return True

    def _beat(self):
        """Beat, Called by protocol"""
        self._lastBeatReceiveTime = datetime.now(pytz.utc)

    def _setNameAndUuid(self, name, uuid):
        """Set Name And Uuid, Called by protocol"""
        self._serverVortexName = name
        self._serverVortexUuid = uuid

    def _checkBeat(self):
        # If we've been asleep, then make note of that (VM suspended)
        checkTimout = (
            datetime.now(pytz.utc) - self._lastHeartBeatCheckTime
        ).seconds > HEART_BEAT_TIMEOUT

        # Has the heart beat expired?
        beatTimeout = (
            datetime.now(pytz.utc) - self._lastBeatReceiveTime
        ).seconds > HEART_BEAT_TIMEOUT

        # Mark that we've just checked it
        self._lastHeartBeatCheckTime = datetime.now(pytz.utc)

        if checkTimout:
            self._lastBeatReceiveTime = datetime.now(pytz.utc)
            return

        if beatTimeout:
            self._reconnectAfterHeartBeatLost()
            return

    def _reconnectAfterHeartBeatLost(self):
        if self._retrying:
            return

        self._retrying = True

        logger.info(
            "VortexServer client dead, reconnecting %s:%s",
            self._server,
            self._port,
        )

        self._close()

    def _close(self):
        if self.__protocol:
            self.__protocol.close()
            self.__protocol = None

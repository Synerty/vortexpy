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
from datetime import datetime
from typing import Union, Optional, List

import pytz
import twisted
from twisted.internet import reactor
from twisted.internet import task
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.error import ConnectionDone, ConnectionRefusedError
from twisted.internet.protocol import connectionDone, ReconnectingClientFactory

from vortex.DeferUtil import vortexLogFailure, isMainThread
from vortex.PayloadEnvelope import PayloadEnvelope, VortexMsgList
from vortex.PayloadPriority import DEFAULT_PRIORITY
from vortex.VortexABC import VortexABC, VortexInfo
from vortex.VortexPayloadProtocol import VortexPayloadProtocol
from vortex.VortexServer import HEART_BEAT_PERIOD
from vortex.VortexWritePushProducer import VortexWritePushProducer

logger = logging.getLogger(name=__name__)


class VortexPayloadTcpClientProtocol(VortexPayloadProtocol):
    def __init__(self, vortexClient=None) -> None:
        VortexPayloadProtocol.__init__(self, logger)
        self._vortexClient = vortexClient

        self._closed = False

        # Start our heart beat
        self._sendBeatLoopingCall = task.LoopingCall(self._sendBeat)
        d = self._sendBeatLoopingCall.start(HEART_BEAT_PERIOD, now=False)
        d.addErrback(lambda f: logger.exception(f.value))

        self._producer: Optional[VortexWritePushProducer] = None

    def _beat(self):
        if self._vortexClient:
            self._vortexClient._beat()

    def _nameAndUuidReceived(self, name, uuid):
        from vortex.VortexFactory import VortexFactory

        VortexFactory._notifyOfVortexStatusChange(name, online=True)

        self._producer.setRemoteVortexName(self._serverVortexName)

        if self._vortexClient:
            self._vortexClient._setNameAndUuid(
                name=self._serverVortexName, uuid=self._serverVortexUuid
            )

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
        self._producer.write(b".", DEFAULT_PRIORITY)

    def write(self, payloadVortexStr: bytes, priority: int = DEFAULT_PRIORITY):
        if not twisted.python.threadable.isInIOThread():
            e = Exception("Write called from NON main thread")
            logger.exception(str(e))
            raise e

        assert not self._closed
        self._producer.write(payloadVortexStr, priority)

    def connectionMade(self):
        self._producer = VortexWritePushProducer(
            self.transport, lambda: self.close()
        )

        # Register the producer if there isn't one already.
        if not self.transport.producer:
            self.transport.registerProducer(self._producer, True)

        # Send a heart beat down the new connection, tell it who we are.
        connectPayloadFilt = {
            PayloadEnvelope.vortexUuidKey: self._vortexClient.uuid,
            PayloadEnvelope.vortexNameKey: self._vortexClient.name,
        }
        self._producer.write(
            PayloadEnvelope(filt=connectPayloadFilt).toVortexMsg(),
            DEFAULT_PRIORITY,
        )

    def connectionLost(self, reason=connectionDone):
        from vortex.VortexFactory import VortexFactory

        VortexFactory._notifyOfVortexStatusChange(
            self._serverVortexName, online=False
        )

        if self._sendBeatLoopingCall.running:
            self._sendBeatLoopingCall.stop()
        self._closed = False

    def close(self):
        self.transport.loseConnection()
        if self._sendBeatLoopingCall.running:
            self._sendBeatLoopingCall.stop()
        self._closed = False


class VortexClientTcp(ReconnectingClientFactory, VortexABC):
    """VortexServer Client
    Connects to a votex server
    """

    RETRY_DELAY = 1.5  # Seconds
    HEART_BEAT_TIMEOUT = 30.0  # Seconds

    # The time it takes after recieving a response from the server to receive the
    INFO_PAYLOAD_TIMEOUT = 5  # Seconds

    # Set the ReconnectingClientFactory max delay
    maxDelay = 1

    def __init__(self, name: str) -> None:
        self._vortexName = name
        self._vortexUuid = str(uuid.uuid1())

        self._server = None
        self._port = None

        self._retrying = False

        self._serverVortexUuid = None
        self._serverVortexName = None

        self._lastBeatReceiveTime = None
        self._lastHeartBeatCheckTime = datetime.now(pytz.utc)

        # Start our heart beat checker
        self._beatLoopingCall = task.LoopingCall(self._checkBeat)

        self._reconnectVortexMsgs = [PayloadEnvelope().toVortexMsg()]

        self.__protocol = None

    #####################################################################################3

    # class EchoClientFactory(ReconnectingClientFactory):
    def startedConnecting(self, connector):
        logger.debug("Started to connect.")

    def buildProtocol(self, addr):

        if self.__protocol:
            self.__protocol.close()
            self.__protocol = None

            logger.debug("Connected.")
        self.resetDelay()
        self.__protocol = VortexPayloadTcpClientProtocol(self)
        return self.__protocol

    def clientConnectionLost(self, connector, reason):
        if not reason.check(ConnectionDone):
            logger.debug("Lost connection.  Reason: %s", reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        if reason.check(ConnectionRefusedError):
            logger.debug("Connection refused (We'll retry)")
        else:
            logger.debug("Connection failed. Reason: %s", reason)
        ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason
        )

    #####################################################################################3

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

    @property
    def name(self):
        return self._vortexName

    @property
    def uuid(self):
        return self._vortexUuid

    def connect(self, server, port):
        self._server = server
        self._port = port

        self._beat()
        d = self._beatLoopingCall.start(5.0, now=False)
        d.addErrback(vortexLogFailure, logger, consumeError=True)

        deferred = Deferred()

        reactor.connectTCP(self._server, self._port, self)

        def checkUuid():
            if self._serverVortexName:
                deferred.callback(True)
            else:
                reactor.callLater(0.1, checkUuid)

        checkUuid()

        return deferred

    def close(self):
        if self._beatLoopingCall and self._beatLoopingCall.running:
            self._beatLoopingCall.stop()
            self._beatLoopingCall = None

        self.stopTrying()
        self.__protocol.close()

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

        if vortexMsgs is None:
            vortexMsgs = self._reconnectVortexMsgs

        if not isinstance(vortexMsgs, list):
            vortexMsgs = [vortexMsgs]

        # Check if the vortexUuid matches the destination uuid
        #
        # if vortexUuid and vortexUuid != self._
        #
        # if not self.__protocol.serverVortexUuid:
        #     return []

        if isMainThread():
            return self._sendVortexMsgLater(vortexMsgs)

        return task.deferLater(reactor, 0, self._sendVortexMsgLater, vortexMsgs)

    @inlineCallbacks
    def _sendVortexMsgLater(self, vortexMsgs: VortexMsgList):
        yield None
        assert self._server
        assert vortexMsgs

        self.vortexMsgs = b""

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
        ).seconds > self.HEART_BEAT_TIMEOUT

        # Has the heart beat expired?
        beatTimeout = (
            datetime.now(pytz.utc) - self._lastBeatReceiveTime
        ).seconds > self.HEART_BEAT_TIMEOUT

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

        if self.__protocol:
            self.__protocol.close()

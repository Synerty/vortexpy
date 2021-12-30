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
from http.cookiejar import CookieJar
from typing import Union, Optional, List
from urllib.parse import urlencode

import pytz
from twisted.internet import reactor, task
from twisted.internet.defer import succeed, Deferred, inlineCallbacks
from twisted.python.failure import Failure
from twisted.web.client import Agent, CookieAgent
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface.declarations import implementer

from vortex.DeferUtil import isMainThread
from vortex.PayloadEnvelope import PayloadEnvelope, VortexMsgList
from vortex.PayloadPriority import DEFAULT_PRIORITY
from vortex.VortexABC import VortexABC, VortexInfo
from vortex.VortexPayloadProtocol import VortexPayloadProtocol

logger = logging.getLogger(name=__name__)


@implementer(IBodyProducer)
class _VortexClientPayloadProducer(object):
    def __init__(self, vortexMsgs) -> None:
        self.vortexMsgs = b""

        for vortexMsg in vortexMsgs:
            self.vortexMsgs += vortexMsg + b"."

        self.length = len(self.vortexMsgs)

    def startProducing(self, consumer):
        consumer.write(self.vortexMsgs)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class VortexPayloadHttpClientProtocol(VortexPayloadProtocol):
    def __init__(self, logger, vortexClient=None):
        VortexPayloadProtocol.__init__(self, logger)
        self._vortexClient = vortexClient

    def _beat(self):
        if self._vortexClient:
            self._vortexClient._beat()

    def _nameAndUuidReceived(self, name, uuid):
        if self._vortexClient:
            self._vortexClient._setNameAndUuid(
                name=self._serverVortexName, uuid=self._serverVortexUuid
            )


class VortexClientHttp(VortexABC):
    """VortexServer Client
    Connects to a votex server
    """

    RETRY_DELAY = 1.5  # Seconds

    # The time it takes after recieving a response from the server to receive the
    INFO_PAYLOAD_TIMEOUT = 5  # Seconds

    def __init__(self, name: str) -> None:
        self._vortexName = name
        self._vortexUuid = str(uuid.uuid1())

        self._server = None
        self._port = None

        self._retrying = False

        self._serverVortexUuid = None
        self._serverVortexName = None

        self._cookieJar = CookieJar()

        self._beatTime = None
        self._beatTimeout = 15.0  # Server beats at 5 seconds

        # Start our heart beat checker
        self._beatLoopingCall = task.LoopingCall(self._checkBeat)

        self._reconnectVortexMsgs = [PayloadEnvelope().toVortexMsg()]

        self.__protocol = None

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

    def connect(self, server, port):
        self._server = server
        self._port = port

        if self._serverVortexName:
            raise Exception("Reconnecting is not implemented")

        self._beat()
        self._beatLoopingCall.start(5.0)

        deferred = Deferred()

        def checkUuid():
            if self._serverVortexName:
                deferred.callback(True)
            else:
                reactor.callLater(0.1, checkUuid)

        checkUuid()
        self.sendVortexMsg()

        return deferred

    def disconnect(self):
        if self._beatLoopingCall:
            if self._beatLoopingCall.running:
                self._beatLoopingCall.stop()
            self._beatLoopingCall = None

        self.__protocol.transport.loseConnection()

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

        def ebSendAgain(failure):
            self._retrying = True
            logger.debug(
                "Retrying send of %s messages : %s",
                len(vortexMsgs),
                failure.value,
            )

            return task.deferLater(
                reactor, self.RETRY_DELAY, self._sendVortexMsgLater, vortexMsgs
            )

        def cbRequest(response):
            if response.code != 200:
                msg = "Connection to vortex %s:%s failed" % (
                    self._server,
                    self._port,
                )
                logger.error(msg)
                return Failure(Exception(msg))

            elif self._retrying:
                logger.info(
                    "VortexServer client %s:%s reconnected",
                    self._server,
                    self._port,
                )

            self._retrying = False
            self.__protocol = VortexPayloadHttpClientProtocol(
                logger, vortexClient=self
            )
            response.deliverBody(self.__protocol)
            return True

        bodyProducer = _VortexClientPayloadProducer(vortexMsgs)

        agent = CookieAgent(Agent(reactor), self._cookieJar)

        args = {"vortexUuid": self._vortexUuid, "vortexName": self._vortexName}

        uri = (
            "http://%s:%s/vortex?%s"
            % (self._server, self._port, urlencode(args))
        ).encode("UTF-8")

        d = agent.request(
            b"POST",
            uri,
            Headers(
                {
                    b"User-Agent": [b"Synerty VortexServer Client"],
                    b"Content-Type": [b"text/plain"],
                }
            ),
            bodyProducer,
        )

        d.addCallback(cbRequest)
        d.addErrback(ebSendAgain)  # Must be after cbRequest
        return d

    def _beat(self):
        """Beat, Called by protocol"""
        self._beatTime = datetime.now(pytz.utc)

    def _setNameAndUuid(self, name, uuid):
        """Set Name And Uuid, Called by protocol"""
        self._serverVortexName = name
        self._serverVortexUuid = uuid

    def _checkBeat(self):
        if (
            not (datetime.now(pytz.utc) - self._beatTime).seconds
            > self._beatTimeout
        ):
            return

        if self._retrying:
            return

        self._retrying = True

        logger.info(
            "VortexServer client dead, reconnecting %s:%s"
            % (self._server, self._port)
        )

        d = self.sendVortexMsg()

        # Add a errback that handles the failure.
        d.addErrback(lambda _: None)

"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from random import random
from urllib.parse import parse_qs
from urllib.parse import urlparse

from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.internet.protocol import connectionDone
from twisted.protocols.policies import WrappingFactory
from twisted.web import resource
from twisted.web.server import NOT_DONE_YET

from txwebsocket.txws import WebSocketProtocol
from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.VortexServerConnection import VortexServerConnection
from .PayloadPriority import DEFAULT_PRIORITY
from .VortexServer import VortexServer
from .VortexWritePushProducer import VortexWritePushProducer

logger = logging.getLogger(name=__name__)


class VortexWebsocketServerProtocol(Protocol):

    _DEBUG_LOGGING = False

    def __init__(self, vortex: VortexServer, addr, httpSession=None) -> None:
        self._vortex = vortex
        self._addr = addr

        self._conn = None

        self._remoteVortexUuid = None
        self._remoteVortexName = None
        self._httpSession = httpSession

        def httpSessionExpiredCallback():
            self._httpSession = None
            self.transport.loseConnection()

        if self._httpSession:
            self._httpSession.notifyOnExpire(httpSessionExpiredCallback)

        # Most messages don't need a buffer, but websockets can split messages
        # around the 128kb mark
        self._receiveBuffer = b""

    def __initConnection(self):
        # self.transport.setBinaryMode(True)

        params = parse_qs(urlparse(self.transport.location).query)

        if "vortexUuid" not in params or "vortexName" not in params:
            raise Exception(
                "This isn't a vortex capable websocket. Check the URL"
            )

        self._remoteVortexUuid = params["vortexUuid"][0]
        self._remoteVortexName = params["vortexName"][0]
        self._conn = VortexServerConnection(
            self._vortex,
            self._remoteVortexUuid,
            self._remoteVortexName,
            self._httpSession,
            self.transport,
            self._addr,
        )

        # noinspection PyProtectedMember
        assert isinstance(self.transport, WebSocketProtocol) and isinstance(
            self.transport._producerForWrite, VortexWritePushProducer
        ), "The Websocket transport is not how we expected it."

        self.transport._producerForWrite.setRemoteVortexName(
            f"{self._remoteVortexName}({self._remoteVortexUuid})"
        )

        # Send a heart beat down the new connection, tell it who we are.
        connectPayloadFilt = {}
        connectPayloadFilt[PayloadEnvelope.vortexUuidKey] = self._vortex.uuid()
        connectPayloadFilt[PayloadEnvelope.vortexNameKey] = self._vortex.name()
        self._conn.write(
            PayloadEnvelope(filt=connectPayloadFilt).toVortexMsg(),
            DEFAULT_PRIORITY,
        )

        self._vortex.connectionOpened(self._httpSession, self._conn)

    def dataReceived(self, data):
        if not self._conn:
            self.__initConnection()

        if self._vortex.isShutdown():
            return None

        if self._httpSession:
            self._httpSession.touch()

        if data == b".":
            self._conn.beatReceived()
            return

        if len(data) > 1 and data[-1] == b".":
            data = data[:-1]

        d = self._processVortexMsg(data)
        d.addErrback(vortexLogFailure, logger, consumeError=True)

    def connectionLost(self, reason=connectionDone):
        if self._DEBUG_LOGGING:
            logger.debug(
                "VortexWebsocketServerProtocol.connectionLost reason=%s", reason
            )
        if self._conn:
            self._conn.transportClosed()
            self._vortex.connectionClosed(self._conn)

    @inlineCallbacks
    def _processVortexMsg(self, vortexMsg: bytes):
        payloadEnvelope = yield PayloadEnvelope().fromVortexMsgDefer(vortexMsg)
        self._vortex.payloadReveived(
            httpSession=self._httpSession,
            vortexUuid=self._remoteVortexUuid,
            vortexName=self._remoteVortexName,
            payload=payloadEnvelope,
        )


class VortexWebsocketServerFactory(Factory):
    protocol = None

    def __init__(self, vortexServer: VortexServer) -> None:
        self._vortexServer = vortexServer

    def buildProtocol(self, addr, httpSession=None):
        # VortexWebSocketUpgradeResource provides this check
        # if we're connected via a HTTP upgrade
        if not httpSession:
            from vortex.VortexFactory import VortexFactory

            if not VortexFactory.canConnect(str(addr)):
                return

        p = VortexWebsocketServerProtocol(self._vortexServer, addr, httpSession)
        p.factory = self
        return p


class VortexWrappedWebSocketFactory(WrappingFactory):
    """
    Factory which wraps another factory to provide WebSockets transports for
    all of its protocols.
    """

    protocol = WebSocketProtocol

    def buildProtocol(self, addr, httpSession):
        return self.protocol(
            self, self.wrappedFactory.buildProtocol(addr, httpSession)
        )


class VortexWebSocketUpgradeResource(resource.Resource):
    """Vortex Websocket Upgrade Resource

    If this resource is hit, it will attempt to upgrade the connection to a websocket.

    """

    isLeaf = 1

    def __init__(self, websocketFactory):
        """Constructor

        @:param websocketFactory: A factory that will build a WebsocketProtocol (above)
        """
        resource.Resource.__init__(self)
        self._websocketFactory = websocketFactory

    def render(self, request):

        from vortex.VortexFactory import VortexFactory

        if not VortexFactory.canConnect(str(request.client.host)):
            request.setResponseCode(503)
            request.setHeader("Retry-After", str(max(10, int(random() * 100))))
            return b"Rejected due to new connections throttling."

        httpSession = request.getSession()

        websocketProtocol = self._websocketFactory.buildProtocol(
            request.client, httpSession
        )
        websocketProtocol.makeConnection(request.channel.transport)
        websocketProtocol.initFromRequest(request)
        request.channel.upgradeToWebsocket(websocketProtocol)

        def unregister():
            request.channel.unregisterProducer()
            websocketProtocol.unregisterProducer()

        producer = VortexWritePushProducer(
            request.channel.transport,
            stopProducingCallback=lambda: unregister(),
            # The websocket frames this before this producer gets it
            writeWholeFrames=False,
            terminateFrameWithDot=False,
            splitFrames=True,
        )
        producer.WARNING_DATA_LENGTH = 10 * 1024 * 1024
        producer.ERROR_DATA_LENGTH = 50 * 1024 * 1024

        unregister()
        request.channel.registerProducer(producer, streaming=True)
        websocketProtocol.registerProducer(producer, streaming=True)

        def closedError(failure):
            # logger.error("Got closedError %s" % failure)
            websocketProtocol.connectionLost(failure.value)

        def closedOk(data):
            pass
            # logger.debug("Got closedOk %s" % data)

        request.notifyFinish().addCallbacks(closedOk, closedError)

        return NOT_DONE_YET

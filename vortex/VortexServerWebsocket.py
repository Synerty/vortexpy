"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from urllib.parse import urlparse, parse_qs

from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import Protocol, connectionDone, Factory
from twisted.protocols.policies import WrappingFactory
from twisted.web import resource
from twisted.web.server import NOT_DONE_YET
from txwebsocket.txws import WebSocketProtocol

from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.VortexServerConnection import VortexServerConnection
from .PayloadPriority import DEFAULT_PRIORITY
from .VortexServer import VortexServer

logger = logging.getLogger(name=__name__)


class VortexWebsocketServerProtocol(Protocol):
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

        self._receiveBuffer += data

        if self._receiveBuffer.endswith(b"."):
            d = self._processVortexMsg(self._receiveBuffer)
            d.addErrback(vortexLogFailure, logger, consumeError=True)
            self._receiveBuffer = b""

    def connectionLost(self, reason=connectionDone):
        if self._conn:
            self._conn.transportClosed()

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
        httpSession = request.getSession()

        websocketProtocol = self._websocketFactory.buildProtocol(
            request.client.host, httpSession
        )
        websocketProtocol.makeConnection(request.channel.transport)
        websocketProtocol.initFromRequest(request)
        request.channel.upgradeToWebsocket(websocketProtocol)

        return NOT_DONE_YET

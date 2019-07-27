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

from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.VortexServerConnection import VortexServerConnection
from .VortexServer import VortexServer

logger = logging.getLogger(name=__name__)



class VortexWebsocketServerProtocol(Protocol):
    def __init__(self, vortex: VortexServer, addr) -> None:
        self._vortex = vortex
        self._addr = addr

        self._conn = None

        self._remoteVortexUuid = None
        self._remoteVortexName = None
        self._httpSession = None

        # Most messages don't need a buffer, but websockets can split messages
        # around the 128kb mark
        self._receiveBuffer = b''

    def __initConnection(self):
        # self.transport.setBinaryMode(True)

        params = parse_qs(urlparse(self.transport.location).query)

        if 'vortexUuid' not in params or 'vortexName' not in params:
            raise Exception("This isn't a vortex capable websocket. Check the URL")

        self._remoteVortexUuid = params['vortexUuid'][0]
        self._remoteVortexName = params['vortexName'][0]
        self._conn = VortexServerConnection(self._vortex,
                                            self._remoteVortexUuid,
                                            self._remoteVortexName,
                                            self._httpSession,
                                            self.transport,
                                            self._addr)

        # Send a heart beat down the new connection, tell it who we are.
        connectPayloadFilt = {}
        connectPayloadFilt[PayloadEnvelope.vortexUuidKey] = self._vortex.uuid()
        connectPayloadFilt[PayloadEnvelope.vortexNameKey] = self._vortex.name()
        self._conn.write(PayloadEnvelope(filt=connectPayloadFilt).toVortexMsg())

        self._vortex.connectionOpened(self._httpSession, self._conn)

    def dataReceived(self, data):

        if not self._conn:
            self.__initConnection()

        if self._vortex.isShutdown():
            return None

        if data == b'.':
            self._conn.beatReceived()
            return

        self._receiveBuffer += data

        if self._receiveBuffer.endswith(b'.'):
            d = self._processVortexMsg(self._receiveBuffer)
            d.addErrback(vortexLogFailure, logger, consumeError=True)
            self._receiveBuffer = b''

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
            payload=payloadEnvelope)


class VortexWebsocketServerFactory(Factory):
    protocol = None

    def __init__(self, vortexServer: VortexServer) -> None:
        self._vortexServer = vortexServer

    def buildProtocol(self, addr):
        p = VortexWebsocketServerProtocol(self._vortexServer, addr)
        p.factory = self
        return p


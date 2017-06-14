"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from datetime import datetime
from struct import pack
from urllib.parse import urlparse, parse_qs

import six
import txws
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import Protocol, connectionDone, Factory

from vortex.DeferUtil import vortexLogFailure
from vortex.VortexServerConnection import VortexServerConnection
from .Payload import Payload
from .VortexConnectionABC import VortexConnectionABC
from .VortexServer import VortexServer, HEART_BEAT_PERIOD, HEART_BEAT_TIMEOUT

logger = logging.getLogger(name=__name__)


def make_hybi07_frame(buf, opcode=0x1):
    """
    Make a HyBi-07 frame.

    This function always creates unmasked frames, and attempts to use the
    smallest possible lengths.
    """

    if len(buf) > 0xffff:
        length = six.b("\x7f%s") % pack(">Q", len(buf))
    elif len(buf) > 0x7d:
        length = six.b("\x7e%s") % pack(">H", len(buf))
    else:
        length = six.b(chr(len(buf)))

    if isinstance(buf, six.text_type):
        buf = buf.encode('utf-8')

    # Always make a normal packet.
    header = chr(0x80 | opcode)
    return six.b(header) + length + buf


txws.make_hybi07_frame = make_hybi07_frame


class VortexWebsocketServerProtocol(Protocol):
    def __init__(self, vortex: VortexServer, addr):
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
        connectPayloadFilt[Payload.vortexUuidKey] = self._vortex.uuid()
        connectPayloadFilt[Payload.vortexNameKey] = self._vortex.name()
        self._conn.write(Payload(filt=connectPayloadFilt).toVortexMsg())

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
        payload = yield Payload().fromVortexMsgDefer(vortexMsg)
        self._vortex.payloadReveived(
            httpSession=self._httpSession,
            vortexUuid=self._remoteVortexUuid,
            vortexName=self._remoteVortexName,
            payload=payload)



class VortexWebsocketServerFactory(Factory):
    protocol = None

    def __init__(self, vortexServer: VortexServer):
        self._vortexServer = vortexServer

    def buildProtocol(self, addr):
        p = VortexWebsocketServerProtocol(self._vortexServer, addr)
        p.factory = self
        return p

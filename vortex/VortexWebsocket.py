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

        self._dataBuffer = b""
        self._remoteVortexUuid = None
        self._remoteVortexName = None
        self._httpSession = None

    def __initConnection(self):
        # self.transport.setBinaryMode(True)

        params = parse_qs(urlparse(self.transport.location).query)

        self._remoteVortexUuid = params['vortexUuid'][0]
        self._remoteVortexName = params['vortexName'][0]
        self._conn = VortexWebsocketConnection(self._vortex,
                                               self._remoteVortexUuid,
                                               self._remoteVortexName,
                                               self._httpSession,
                                               self.transport,
                                               self._addr)

        self._vortex.connectionOpened(self._httpSession, self._conn)

    def dataReceived(self, data):

        if not self._conn:
            self.__initConnection()

        if self._vortex.isShutdown():
            return None

        if data in ('.', b'.'):
            self._conn.beatReceived()
            return

        # self._dataBuffer += data
        #
        # while b"." in self._dataBuffer:
        #     index = self._dataBuffer.index(b".")
        #     chunk = self._dataBuffer[:index]
        #     self._dataBuffer = self._dataBuffer[index + 1:]
        #
        #     self._processVortexMsg(chunk)

        d = self._processVortexMsg(data)
        d.addErrback(lambda f: logger.exception(f.value))

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


class VortexWebsocketConnection(VortexConnectionABC):
    def __init__(self, vortexServer: VortexServer,
                 remoteVortexUuid: str,
                 remoteVortexName: str,
                 httpSession, transport,
                 addr):
        VortexConnectionABC.__init__(self,
                                     logger,
                                     vortexServer,
                                     remoteVortexUuid=remoteVortexUuid,
                                     remoteVortexName=remoteVortexName,
                                     httpSessionUuid=httpSession)

        self._transport = transport
        self._addr = addr

        # Start our heart beat
        self._beatLoopingCall = task.LoopingCall(self._beat)
        d = self._beatLoopingCall.start(HEART_BEAT_PERIOD)
        d.addErrback(lambda f: logger.exception(f.value))

        self._lastHeartBeatTime = datetime.utcnow()

    def beatReceived(self):
        self._lastHeartBeatTime = datetime.utcnow()

    def _beat(self):
        if self._closed:
            self._beatLoopingCall.stop()
            return

        if (datetime.utcnow() - self._lastHeartBeatTime).seconds > HEART_BEAT_TIMEOUT:
            self._beatLoopingCall.stop()
            self.close()
            return

        # Send the heartbeats
        self._transport.write(b'.')

    @property
    def ip(self):
        return self._addr.host

    @property
    def port(self):
        return self._addr.port

    def write(self, payloadVortexStr: bytes):
        assert not self._closed
        self._transport.write(payloadVortexStr)

    def close(self):
        self._transport.loseConnection()

    def transportClosed(self):
        VortexConnectionABC.close(self)


class VortexWebsocketServerFactory(Factory):
    protocol = None

    def __init__(self, vortexServer: VortexServer):
        self._vortexServer = vortexServer

    def buildProtocol(self, addr):
        p = VortexWebsocketServerProtocol(self._vortexServer, addr)
        p.factory = self
        return p

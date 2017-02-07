"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from typing import Union
from urllib.parse import urlparse, parse_qs

from twisted.internet.defer import inlineCallbacks, succeed
from twisted.internet.protocol import connectionDone, Factory

from vortex.VortexPayloadProtocol import VortexPayloadProtocol
from vortex.VortexServerConnection import VortexServerConnection
from .Payload import Payload, VortexMsgList
from .VortexServer import VortexServer

logger = logging.getLogger(name=__name__)




class VortexTcpServerProtocol(VortexPayloadProtocol):
    def __init__(self, vortex: VortexServer, addr):
        VortexPayloadProtocol.__init__(self, logger)
        self._vortex = vortex
        self._addr = addr

        self._conn = None

        self._dataBuffer = b""
        self._remoteVortexUuid = None
        self._remoteVortexName = None
        self._httpSession = None

    def _beat(self):
        if self._conn:
            self._conn.beatReceived()

    def _nameAndUuidReceived(self, name, uuid):
        # self.transport.setBinaryMode(True)

        self._remoteVortexUuid = uuid
        self._remoteVortexName = name
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

    def _createResponseSenderCallable(self):
        def sendResponse(vortexMsgs: Union[VortexMsgList, bytes]):
            if isinstance(vortexMsgs, bytes):
                vortexMsgs = [vortexMsgs]

            for vortexMsg in vortexMsgs:
                self._conn.write(vortexMsg)

            return succeed(True)

        return sendResponse

    def dataReceived(self, data):

        if self._vortex.isShutdown():
            return None

        VortexPayloadProtocol.dataReceived(self, data)


    def connectionLost(self, reason=connectionDone):
        if self._conn:
            self._conn.transportClosed()

        VortexPayloadProtocol.connectionLost(self, reason)



class VortexTcpServerFactory(Factory):
    protocol = None

    def __init__(self, vortexServer: VortexServer):
        self._vortexServer = vortexServer

    def buildProtocol(self, addr):
        p = VortexTcpServerProtocol(self._vortexServer, addr)
        p.factory = self
        return p

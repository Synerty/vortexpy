"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

import collections
import logging
from _pyio import BytesIO

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from .Payload import Payload
from .VortexConnectionABC import VortexConnectionABC
from .VortexServer import VortexServer

logger = logging.getLogger(name=__name__)


class VortexResource(Resource):
    """ VortexServer Resource
      This resource is the server endpoint for the vortex.
    """
    isLeaf = True

    def __init__(self, vortex: VortexServer):
        Resource.__init__(self)
        self.__vortex = vortex

    def render_GET(self, request):
        request.code = 405
        return b"You have reached the vortex. It only likes POST methods."

    def render_POST(self, request):
        remoteVortexUuid = request.args[b'vortexUuid'][0].decode()
        remoteVortexName = request.args[b'vortexName'][0].decode()

        if self.__vortex.isShutdown():
            return None

        session = request.getSession()
        conn = VortexResourceConnection(self.__vortex,
                                        remoteVortexUuid,
                                        remoteVortexName,
                                        request, session)

        data = request.content.read()
        if len(data):
            for vortexStr in data.strip(b'.').split(b'.'):
                self._processVortexMsg(session, conn, vortexStr.decode("UTF-8"))

        # Request will be around for a while, do some cleanups
        request.content = BytesIO()
        request.args = {}

        self.__vortex.connectionOpened(session, conn)

        def connClosed(err):
            logger.debug("VortexServer connection ended by client")
            self.__vortex.connectionClosed(conn)

        request.notifyFinish().addErrback(connClosed)

        return NOT_DONE_YET

    @inlineCallbacks
    def _processVortexMsg(self, httpSession, conn, vortexMsg):
        payload = yield Payload().fromVortexMsgDefer(vortexMsg)
        self.__vortex.payloadReveived(
            httpSession=httpSession,
            vortexUuid=conn.vortexUuid,
            vortexName=conn.vortexName,
            payload=payload)


class VortexResourceConnection(VortexConnectionABC):
    def __init__(self, vortexServer: VortexServer,
                 remoteVortexUuid: str,
                 remoteVortexName: str,
                 request, session):
        VortexConnectionABC.__init__(self,
                                     logger,
                                     vortexServer,
                                     remoteVortexUuid=remoteVortexUuid,
                                     remoteVortexName=remoteVortexName,
                                     httpSessionUuid=session.uid)

        self._request = request
        self._request.responseHeaders.setRawHeaders(b'content-type'
                                                    , [b'text/text'])

        self._buffer = collections.deque()

        self._requestReadyCheckTimer = task.LoopingCall(self._checkIfRequestIsReady)
        self._requestReadyCheckTimer.start(0.001)

    @property
    def ip(self):
        return self._request.client.host

    @property
    def port(self):
        return self._request.client.port

    def _checkIfRequestIsReady(self):
        # This seems to be the fingerprint of the fact that request will not write
        # our data yet.
        if self._request.chunked and not self._request.startedWriting:
            return

        if self._requestReadyCheckTimer.running:
            self._requestReadyCheckTimer.stop()

        while self._buffer:
            self._write(self._buffer.popleft())
        self._buffer = None

    def write(self, payloadVortexStr: bytes):
        assert not self._closed
        # If we have a buffer, write it.
        if self._buffer:
            self._buffer.append(payloadVortexStr)
        else:
            self._write(payloadVortexStr)

    def _write(self, payloadVortexStr: bytes):
        if self._request.finished:
            logger.error("VORTEX CLOSED, CAN'T WRITE PAYLOAD")
            self._closed = True
            self._vortexServer.connectionClosed(self)
            return
        self._request.write(payloadVortexStr)
        self._request.write(b'.')

    def close(self):
        self._closed = True
        self._request.finish()

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
from .Vortex import Vortex
from .VortexConnection import VortexConnection

logger = logging.getLogger(name=__name__)


class VortexResource(Resource):
    """ Vortex Resource
      This resource is the server endpoint for the vortex.
    """
    isLeaf = True

    def render_GET(self, request):
        request.code = 405
        return b"You have reached the vortex. It only likes POST methods."

    def render_POST(self, request):
        # We don't care about the arguments, ever. (In this design anyway)

        if Vortex().isShutdown():
            return None

        session = request.getSession()
        conn = VortexResourceConnection(request, session)

        data = request.content.read()
        if len(data):
            for vortexStr in data.strip(b'.').split(b'.'):
                self._processVortexMsg(session, conn, vortexStr.decode("UTF-8"))

        # Request will be around for a while, do some cleanups
        request.content = BytesIO()
        request.args = {}

        Vortex().connectionOpened(session, conn)

        def connClosed(err):
            logger.debug("Vortex connection ended by client")
            Vortex().connectionClosed(conn)

        request.notifyFinish().addErrback(connClosed)

        return NOT_DONE_YET

    @inlineCallbacks
    def _processVortexMsg(self, session, conn, vortexMsg):
        payload = yield Payload().fromVortexMsgDefer(vortexMsg)
        Vortex().payloadReveived(session, conn, payload)


class VortexResourceConnection(VortexConnection):
    def __init__(self, request, session):
        VortexConnection.__init__(self,
                                  logger,
                                  request.args[b'vortexUuid'][0],
                                  httpSessionUuid=session.uid)

        self._request = request
        self._request.responseHeaders.setRawHeaders(b'content-type'
                                                    , [b'text/text'])

        self._buffer = collections.deque()

        self._requestReadyCheckTimer = task.LoopingCall(self._checkIfRquestIsReady)
        self._requestReadyCheckTimer.start(0.001)

    @property
    def ip(self):
        return self._request.client.host

    @property
    def port(self):
        return self._request.client.port

    def _checkIfRquestIsReady(self):
        # This seems to be the fingerprint of the fact that request will not write
        # our data yet.
        if self._request.chunked and not self._request.startedWriting:
            return

        if self._requestReadyCheckTimer.running:
            self._requestReadyCheckTimer.stop()

        while self._buffer:
            self._write(self._buffer.popleft())
        self._buffer = None

    def write(self, payloadVortexStr):
        assert not self._closed
        # If we have a buffer, write it.
        if self._buffer:
            self._buffer.append(payloadVortexStr)
        else:
            self._write(payloadVortexStr)

    def _write(self, payloadVortexStr):
        if self._request.finished:
            logger.error("VORTEX CLOSED, CAN'T WRITE PAYLOAD")
            self._closed = True
            Vortex().connectionClosed(self)
            return
        self._request.write(payloadVortexStr)
        self._request.write(b'.')

    def close(self):
        self._closed = True
        self._request.finish()


def createVortexResource(**kwargs):
    return VortexResource()

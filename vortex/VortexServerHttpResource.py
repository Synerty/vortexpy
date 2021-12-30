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
from typing import Deque

from twisted.internet import task
from twisted.internet.defer import inlineCallbacks
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.VortexServer import HEART_BEAT_PERIOD
from .PayloadPriority import DEFAULT_PRIORITY
from .VortexConnectionABC import VortexConnectionABC
from .VortexServer import VortexServer

logger = logging.getLogger(name=__name__)


class VortexServerHttpResource(Resource):
    """VortexServer Resource
    This resource is the server endpoint for the vortex.
    """

    isLeaf = True

    def __init__(self, vortex: VortexServer) -> None:
        Resource.__init__(self)
        self.__vortex = vortex

    def render_GET(self, request):
        request.code = 405
        return b"You have reached the vortex. It only likes POST methods."

    def render_POST(self, request):
        remoteVortexUuid = request.args[b"vortexUuid"][0].decode()
        remoteVortexName = request.args[b"vortexName"][0].decode()

        if self.__vortex.isShutdown():
            return None

        httpSession = request.getSession()
        conn = VortexResourceConnection(
            self.__vortex, remoteVortexUuid, remoteVortexName, request
        )

        # Send a heart beat down the new connection, tell it who we are.
        connectPayloadFilt = {}
        connectPayloadFilt[PayloadEnvelope.vortexUuidKey] = self.__vortex.uuid()
        connectPayloadFilt[PayloadEnvelope.vortexNameKey] = self.__vortex.name()
        conn.write(PayloadEnvelope(filt=connectPayloadFilt).toVortexMsg())

        data = request.content.read()
        if len(data):
            for vortexStr in data.strip(b".").split(b"."):
                self._processVortexMsg(
                    httpSession, conn, vortexStr.decode("UTF-8")
                )

        # Request will be around for a while, do some cleanups
        request.content = BytesIO()
        request.args = {}

        self.__vortex.connectionOpened(httpSession, conn)

        def connClosed(err):
            logger.debug("VortexServer connection ended by client")
            self.__vortex.connectionClosed(conn)

        request.notifyFinish().addErrback(connClosed)

        return NOT_DONE_YET

    @inlineCallbacks
    def _processVortexMsg(self, httpSession, conn, vortexMsg):
        payload = yield PayloadEnvelope().fromVortexMsgDefer(vortexMsg)
        self.__vortex.payloadReveived(
            httpSession=httpSession,
            vortexUuid=conn.remoteVortexUuid,
            vortexName=conn.remoteVortexName,
            payload=payload,
        )


class VortexResourceConnection(VortexConnectionABC):
    def __init__(
        self,
        vortexServer: VortexServer,
        remoteVortexUuid: str,
        remoteVortexName: str,
        request,
    ) -> None:
        VortexConnectionABC.__init__(
            self,
            logger,
            vortexServer,
            remoteVortexUuid=remoteVortexUuid,
            remoteVortexName=remoteVortexName,
            httpSessionUuid=request.getSession().uid,
        )

        self._request = request
        self._request.responseHeaders.setRawHeaders(
            b"content-type", [b"text/text"]
        )

        self._buffer: Deque[bytes] = collections.deque()

        self._requestReadyCheckTimer = task.LoopingCall(
            self._checkIfRequestIsReady
        )
        self._requestReadyCheckTimer.start(0.001)

        # Start our heart beat
        self._beatLoopingCall = task.LoopingCall(self._beat)
        d = self._beatLoopingCall.start(HEART_BEAT_PERIOD)
        d.addErrback(lambda f: logger.exception(f.value))

    def _beat(self):
        if (
            self._closed
            or self._request.finished
            or self._request.channel is None
        ):
            self._beatLoopingCall.stop()
            return

        # Send the heartbeats
        self._request.write(b".")

        # Touch the session
        self._request.getSession().touch()

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

    def write(self, payloadVortexStr: bytes, priority: int = DEFAULT_PRIORITY):
        assert not self._closed
        # If we have a buffer, write it.
        if self._buffer:
            self._buffer.append(payloadVortexStr)
        else:
            self._write(payloadVortexStr)

    def _write(self, payloadVortexStr: bytes):
        if self._request.finished or self._request.channel is None:
            logger.error("VORTEX CLOSED, CAN'T WRITE PAYLOAD")
            self._closed = True
            self._vortexServer.connectionClosed(self)
            return

        self._request.write(payloadVortexStr)
        self._request.write(b".")

    def close(self):
        if self._beatLoopingCall.running:
            self._beatLoopingCall.stop()

        self._closed = True
        self._request.finish()

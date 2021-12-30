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

import pytz
from twisted.internet import task

from .PayloadPriority import DEFAULT_PRIORITY
from .VortexConnectionABC import VortexConnectionABC
from .VortexServer import VortexServer, HEART_BEAT_PERIOD, HEART_BEAT_TIMEOUT
from .VortexWritePushProducer import VortexWritePushProducer

logger = logging.getLogger(name=__name__)


class VortexServerConnection(VortexConnectionABC):
    def __init__(
        self,
        vortexServer: VortexServer,
        remoteVortexUuid: str,
        remoteVortexName: str,
        httpSession,
        transport,
        addr,
    ) -> None:
        VortexConnectionABC.__init__(
            self,
            logger,
            vortexServer,
            remoteVortexUuid=remoteVortexUuid,
            remoteVortexName=remoteVortexName,
            httpSessionUuid=httpSession,
        )

        self._lastHeartBeatTime = datetime.now(pytz.utc)
        self._lastHeartBeatCheckTime = datetime.now(pytz.utc)

        self._transport = transport
        self._addr = addr

        # Start our heart beat
        self._beatLoopingCall = task.LoopingCall(self._beat)
        d = self._beatLoopingCall.start(HEART_BEAT_PERIOD, now=False)
        d.addErrback(lambda f: logger.exception(f.value))

        self._producer = None

        # Register the producer if there isn't one already.
        # The websocket server already has one.
        if not self._transport.producer:
            self._producer = VortexWritePushProducer(
                transport, lambda: self.close(), remoteVortexName
            )

            transport.registerProducer(self._producer, True)

    def beatReceived(self):
        self._lastHeartBeatTime = datetime.now(pytz.utc)

    def _beat(self):
        # If we're closed, do nothing
        if self._closed:
            if self._beatLoopingCall.running:
                self._beatLoopingCall.stop()
            return

        beatTimeout = (
            datetime.now(pytz.utc) - self._lastHeartBeatTime
        ).seconds > HEART_BEAT_TIMEOUT

        # If we've been asleep, then make note of that (VM suspended)
        checkTimout = (
            datetime.now(pytz.utc) - self._lastHeartBeatCheckTime
        ).seconds > HEART_BEAT_TIMEOUT

        # Mark that we've just checked it
        self._lastHeartBeatCheckTime = datetime.now(pytz.utc)

        if checkTimout:
            self._lastHeartBeatTime = datetime.now(pytz.utc)
            return

        # If we havn't heard from the client, then close the connection
        if beatTimeout:
            self._beatLoopingCall.stop()
            self.close()
            return

        self._write(b".", DEFAULT_PRIORITY)

    @property
    def ip(self):
        return self._addr.host

    @property
    def port(self):
        return self._addr.port

    def write(self, payloadVortexStr: bytes, priority: int = DEFAULT_PRIORITY):
        assert not self._closed
        self._write(payloadVortexStr, priority)

    def _write(self, payloadVortexStr: bytes, priority: int):
        if self._producer:
            self._producer.write(payloadVortexStr, priority)
        else:
            self._transport.write(payloadVortexStr)

    def close(self):
        if self._beatLoopingCall.running:
            self._beatLoopingCall.stop()

        self._transport.loseConnection()

    def transportClosed(self):
        self._producer.close()
        VortexConnectionABC.close(self)

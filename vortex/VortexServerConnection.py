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

from .VortexConnectionABC import VortexConnectionABC
from .VortexServer import VortexServer, HEART_BEAT_PERIOD, HEART_BEAT_TIMEOUT

logger = logging.getLogger(name=__name__)


class VortexServerConnection(VortexConnectionABC):
    def __init__(self, vortexServer: VortexServer,
                 remoteVortexUuid: str,
                 remoteVortexName: str,
                 httpSession, transport,
                 addr) -> None:
        VortexConnectionABC.__init__(self,
                                     logger,
                                     vortexServer,
                                     remoteVortexUuid=remoteVortexUuid,
                                     remoteVortexName=remoteVortexName,
                                     httpSessionUuid=httpSession)

        self._lastHeartBeatTime = datetime.now(pytz.utc)

        self._transport = transport
        self._addr = addr

        # Start our heart beat
        self._beatLoopingCall = task.LoopingCall(self._beat)
        d = self._beatLoopingCall.start(HEART_BEAT_PERIOD, now=False)
        d.addErrback(lambda f: logger.exception(f.value))

    def beatReceived(self):
        self._lastHeartBeatTime = datetime.now(pytz.utc)

    def _beat(self):
        # If we're closed, do nothing
        if self._closed:
            self._beatLoopingCall.stop()
            return

        # If we havn't heard from the client, then close the connection
        if (datetime.now(pytz.utc) - self._lastHeartBeatTime).seconds > HEART_BEAT_TIMEOUT:
            self._beatLoopingCall.stop()
            self.close()
            return

        # Otherwise, Send the heartbeats
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
        self._transport.write(b'.')

    def close(self):
        self._transport.loseConnection()

    def transportClosed(self):
        VortexConnectionABC.close(self)

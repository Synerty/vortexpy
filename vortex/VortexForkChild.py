"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging

import sys, os

from twisted.internet import task
from twisted.internet.protocol import connectionDone
from twisted.internet.stdio import StandardIO

from VortexPayloadClientProtocol import VortexPayloadClientProtocol
from Vortex import Vortex
from VortexForkParent import VORTEX_PARENT_IN_FD

logger = logging.getLogger(name=__name__)


class ModData:
    standardIo = None
    killCheckLoopingCall = None

class ForkChildVortexClientProtocol(VortexPayloadClientProtocol):

    def connectionLost(self, reason=connectionDone):
        exit(0)

class ForkChildVortex(object):
    ''' VortexServer
    The static instance of the controller
    '''

    def __init__(self, vortexClientProtocol):
        assert isinstance(vortexClientProtocol, VortexPayloadClientProtocol)
        self._vortexClientProtocol = vortexClientProtocol

    def uuid(self):
        return self._vortexClientProtocol.serverVortexUuid

    def isShutdown(self):
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError

    def vortexUuids(self):
        raise NotImplementedError

    def isVortexAlive(self, vortexUuid):
        raise NotImplementedError

    def vortexClientIpPort(self, vortexUuid):
        raise NotImplementedError

    def _beat(self):
        raise NotImplementedError

    def connectionOpened(self, session, vortexConnection):
        raise NotImplementedError

    def connectionClosed(self, conn):
        raise NotImplementedError

    def _sessionExpired(self, sessionUuid):
        raise NotImplementedError

    def payloadReveived(self, session, conn, payload):
        raise NotImplementedError

    def send(self, payload, vortexUuid=None):
        # Everthing gets sent to the parent process vortex regardless of the vortexUuid
        self.sendVortexMsg(vortexMsg=payload.toVortexMsg(compressionLevel=0),
                           vortexUuid=vortexUuid)

    def sendVortexMsg(self, vortexMsg, vortexUuid=None):
        # Everthing gets sent to the parent process vortex regardless of the vortexUuid
        ModData.standardIo.write(vortexMsg)
        ModData.standardIo.write('.')

def _parentPidCheck():
    if os.getppid() == 1:
        exit(0)


def rapuiSetupForkChildVortexClient(logger):
    if not 'childprocess' in sys.argv:
        return

    protocol = ForkChildVortexClientProtocol(logger=logger)
    ModData.standardIo = StandardIO(protocol, stdout=VORTEX_PARENT_IN_FD)
    Vortex._instance = ForkChildVortex(protocol)

    ModData.killCheckLoopingCall = task.LoopingCall(_parentPidCheck)
    ModData.killCheckLoopingCall.start(0.2)

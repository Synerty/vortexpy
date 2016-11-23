"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
import subprocess
from collections import defaultdict

import os
import uuid

import sys

from os import linesep
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ProcessProtocol

from Payload import Payload
from PayloadIO import PayloadIO
from Vortex import Vortex
from VortexConnection import VortexConnection

# Don't use stdout as log messages may go into that.
from VortexPayloadServerProtocol import VortexPayloadServerProtocol

VORTEX_PARENT_IN_FD = 3


# VORTEX_PARENT_OUT_FD = stdout

class VortexForkConnection(VortexConnection):
    def __init__(self, logger, vortexUuid, protocol):
        VortexConnection.__init__(self, logger, vortexUuid)
        self._protocol = protocol

    def write(self, payloadVortexStr):
        self._protocol.writeToChild(payloadVortexStr)
        self._protocol.writeToChild('.')


class VortexForkParentProtocol(ProcessProtocol, VortexPayloadServerProtocol):
    def __init__(self, logger):
        VortexPayloadServerProtocol.__init__(self)
        self._logger = logger

        self._vortexUuid = str(uuid.uuid1())
        self._vortexConnection = VortexForkConnection(self._logger,
                                                      self._vortexUuid,
                                                      self)

        self._subprocLogs = defaultdict(str)

    def writeToChild(self, data):
        self.transport.write(data)

    def connectionMade(self):
        Vortex().connectionOpened(None, self._vortexConnection)

    def childDataReceived(self, childFD, data):
        if childFD in (1, 2):
            self._subprocLogs[childFD] += data
            self._logSubproc()
            return

        assert childFD == VORTEX_PARENT_IN_FD

        self._data += data
        self._processData()

    def _logSubproc(self, purge=True):
        for childFD, data in list(self._subprocLogs.items()):
            self._subprocLogs[childFD] = ''

            logFunc = self._logger.debug if childFD == 1 else self._logger.error

            for line in data.splitlines(True):
                if line[-1] != linesep and not purge:
                    self._subprocLogs[childFD] = line
                    break

                logFunc(line.strip())

    def childConnectionLost(self, childFD):
        if childFD != VORTEX_PARENT_IN_FD:
            return

        self._logSubproc(True)
        self._processData()

    def processExited(self, reason):
        self._vortexConnection.close()
        self._vortexConnection = None
        self._logger.debug("processExited, status %d", reason.value.exitCode)

    def processEnded(self, reason):
        self._logger.debug("processEnded, status %d", reason.value.exitCode)


def rapuiSpawnChildProcess(moduleFileName, logger):
    childFDs = {0: 'w',
                1: 'r',
                2: 'r',
                VORTEX_PARENT_IN_FD: 'r'}

    python = sys.executable
    env = os.environ.data
    # Unbuffered, otherwise messages get held up
    args = ['python', '-u', moduleFileName, 'childprocess']

    protocol = VortexForkParentProtocol(logger)
    reactor.spawnProcess(processProtocol=protocol,
                         executable=python,
                         args=args,
                         env=env,
                         # path=path,
                         childFDs=childFDs)

def killAllChildProcs():
    # Kill existing children, if this is a software update restart they still exist
    subprocess.call(['pkill', '-9', '-P', str(os.getpid())])
"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

import logging
from abc import ABCMeta, abstractmethod

from .VortexServer import VortexServer

logger = logging.getLogger(name=__name__)


class VortexConnectionABC(metaclass=ABCMeta):
    def __init__(self, logger,
                 vortexServer: VortexServer,
                 remoteVortexUuid:str,
                 remoteVortexName:str,
                 httpSessionUuid=None):
        self._vortexServer = vortexServer
        self._logger = logger
        self._remoteVortexUuid = remoteVortexUuid
        self._remoteVortexName = remoteVortexName
        self._closed = False
        self._httpSessionUuid = httpSessionUuid

    @property
    def httpSessionUuid(self):
        return self._httpSessionUuid

    @property
    def vortexUuid(self):
        """ Vortex UUID

        The vortex UUID of the remote vortex which this connection represents.

        """
        return self._remoteVortexUuid

    @property
    def vortexName(self):
        """ Vortex Name

        The name of the remote vortex which this connection represents.

        """
        return self._remoteVortexName

    @abstractmethod
    def write(self, payloadVortexStr):
        """ Write

        EG

        > assert not self._closed
        > self._request.write(payloadVortexStr)
        > self._request.write('.')

        """

    def close(self):
        self._vortexServer.connectionClosed(self)
        self._closed = True

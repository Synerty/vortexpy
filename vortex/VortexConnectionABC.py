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
from datetime import datetime

import pytz

from .PayloadPriority import DEFAULT_PRIORITY
from .VortexServer import VortexServer

logger = logging.getLogger(name=__name__)


class VortexConnectionABC(metaclass=ABCMeta):
    def __init__(
        self,
        logger,
        vortexServer: VortexServer,
        remoteVortexUuid: str,
        remoteVortexName: str,
        httpSessionUuid=None,
    ) -> None:
        self._vortexServer = vortexServer
        self._logger = logger
        self._remoteVortexUuid = remoteVortexUuid
        self._remoteVortexName = remoteVortexName
        self._closed = False
        self._timedOut = False
        self._httpSessionUuid = httpSessionUuid
        self._connectDateTime = datetime.now(pytz.UTC)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def timedOut(self) -> bool:
        return self._timedOut

    @property
    def connectDateTime(self) -> datetime:
        return self._connectDateTime

    @property
    def httpSessionUuid(self):
        return self._httpSessionUuid

    @property
    def remoteVortexUuid(self):
        """Vortex UUID

        The vortex UUID of the remote vortex which this connection represents.

        """
        return self._remoteVortexUuid

    @property
    def remoteVortexName(self):
        """Vortex Name

        The name of the remote vortex which this connection represents.

        """
        return self._remoteVortexName

    @abstractmethod
    def write(self, payloadVortexStr: bytes, priority: int = DEFAULT_PRIORITY):
        """Write

        EG

        > assert not self._closed
        > self._request.write(payloadVortexStr)
        > self._request.write('.')

        """

    def close(self):
        self._vortexServer.connectionClosed(self)
        self._closed = True

"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

import logging

from .Vortex import Vortex

logger = logging.getLogger(name=__name__)


class VortexConnection(object):
    def __init__(self, logger, vortexUuid, httpSessionUuid=None):
        self._logger = logger
        self._vortexUuid = vortexUuid
        self._closed = False
        self._httpSessionUuid = httpSessionUuid

    @property
    def httpSessionUuid(self):
        return self._vortexUuid

    @property
    def vortexUuid(self):
        return self._vortexUuid

    def write(self, payloadVortexStr):
        ''' Write

        EG

        > assert not self._closed
        > self._request.write(payloadVortexStr)
        > self._request.write('.')

        '''
        raise NotImplementedError

    def close(self):
        Vortex().connectionClosed(self)
        self._closed = True

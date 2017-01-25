"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import json
import logging
import zlib
from base64 import b64encode, b64decode
from datetime import datetime
from typing import List

from vortex.DeferUtil import deferToThreadWrapWithLogger
from .Jsonable import Jsonable
from .SerialiseUtil import T_RAPUI_PAYLOAD

logger = logging.getLogger(__name__)

PayloadList = List['Payload']
VortexMsgList = List[bytes]


class Payload(Jsonable):
    ''' Payload
    This object represents a hierarchy of data transferred between client and server
    '''
    __fieldNames__ = ['filt', 'replyFilt', 'tuples', 'result', 'date']
    __rapuiSerialiseType__ = T_RAPUI_PAYLOAD

    vortexUuidKey = '__vortexUuid__'
    vortexNameKey = '__vortexName__'

    def __init__(self, filt=None, replyFilt=None, tuples=None, result=None):
        '''
        Constructor
        '''
        self.to = None
        self.filt = {} if filt is None else filt
        self.replyFilt = {} if replyFilt is None else replyFilt
        self.tuples = [] if tuples is None else tuples
        self.result = result
        self.date = datetime.utcnow()

        if isinstance(filt, str):
            self.filt = {self.filt: None}

        if not isinstance(self.tuples, list):
            self.tuples = [self.tuples]

    def isEmpty(self):
        return ((not self.filt
                 or (self.vortexNameKey in self.filt
                     and self.vortexUuidKey in self.filt
                     and len(self.filt) == 2))
                and not self.replyFilt
                and not self.tuples
                and not self.result)

    # -------------------------------------------
    # JSON Related methods
    def _fromJson(self, jsonStr):
        jsonDict = json.loads(jsonStr)

        assert (jsonDict[Jsonable.JSON_CLASS_TYPE] == self.__rapuiSerialiseType__)
        return self.fromJsonDict(jsonDict)

    def _toJson(self) -> str:
        return json.dumps(self.toJsonDict())

    # -------------------------------------------
    # VortexServer Message Methods
    def toVortexMsg(self, compressionLevel: int = 9) -> bytes:
        jsonStr = self._toJson()
        return b64encode(zlib.compress(jsonStr.encode("UTF-8"), compressionLevel))

    @deferToThreadWrapWithLogger(logger)
    def toVortexMsgDefer(self, compressionLevel: int = 9) -> bytes:
        return self.toVortexMsg(compressionLevel=compressionLevel)

    def fromVortexMsg(self, vortexMsg: bytes):
        jsonStr = zlib.decompress(b64decode(vortexMsg)).decode()
        return self._fromJson(jsonStr)

    @deferToThreadWrapWithLogger(logger)
    def fromVortexMsgDefer(self, vortexMsg: bytes):
        return self.fromVortexMsg(vortexMsg)

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

from twisted.internet.threads import deferToThread

from .Jsonable import Jsonable
from .SerialiseUtil import T_RAPUI_PAYLOAD

logger = logging.getLogger(__name__)


def printFailure(failure):
    logger.error(failure)
    return failure


def deferToThreadWrap(funcToWrap):
    def func(*args, **kwargs):
        d = deferToThread(funcToWrap, *args, **kwargs)
        d.addErrback(printFailure)
        return d

    return func


class Payload(Jsonable):
    ''' Payload
    This object represents a hierarchy of data transferred between client and server
    '''
    __fieldNames__ = ['filt', 'replyFilt', 'tuples', 'result', 'date']
    __rapuiSerialiseType__ = T_RAPUI_PAYLOAD

    vortexUuidKey = '__vortexUuid__'

    def __init__(self, filt=None, replyFilt=None, tuples=None, result=None):
        '''
        Constructor
        '''
        self.filt = filt if filt != None else {}
        self.replyFilt = replyFilt if replyFilt != None else {}
        self.tuples = tuples if tuples != None else []
        self.result = result
        self.date = datetime.utcnow()

        if isinstance(filt, str):
            self.filt = {self.filt: None}

        if not isinstance(self.tuples, list):
            self.tuples = [self.tuples]

    def isEmpty(self):
        return ((not self.filt
                 or (self.vortexUuidKey in self.filt and len(self.filt) == 1))
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
    # Vortex Message Methods
    def toVortexMsg(self, compressionLevel: int = 9) -> bytes:
        # return b64encode(zlib.compress(self._toXmlDocStr(), compressionLevel))
        jsonStr = self._toJson()
        return b64encode(zlib.compress(jsonStr.encode("UTF-8"), compressionLevel))

    @deferToThreadWrap
    def toVortexMsgDefer(self, compressionLevel: int = 9) -> bytes:
        return self.fromVortexMsg(compressionLevel=compressionLevel)

    def fromVortexMsg(self, vortexMsg: bytes):
        # return self._fromXmlDocStr(zlib.decompress(b64decode(vortexMsg)))
        jsonStr = zlib.decompress(b64decode(vortexMsg)).decode("UTF-8")
        return self._fromJson(jsonStr)

    @deferToThreadWrap
    def fromVortexMsgDefer(self, vortexMsg: bytes):
        return self.fromVortexMsg(vortexMsg)

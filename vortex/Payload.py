"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
import zlib
from base64 import b64encode, b64decode
from datetime import datetime
from typing import List, Optional, Dict

import pytz
import ujson

from vortex.DeferUtil import deferToThreadWrapWithLogger, noMainThread
from .Jsonable import Jsonable
from .SerialiseUtil import T_RAPUI_PAYLOAD

logger = logging.getLogger(__name__)

PayloadList = List["Payload"]
EncodedPayloadList = List[bytes]


class Payload(Jsonable):
    """Payload
    This object represents a hierarchy of data transferred between client and server
    """

    __fieldNames__ = ["filt", "tuples", "date"]
    __rapuiSerialiseType__ = T_RAPUI_PAYLOAD

    def __init__(self, filt: Optional[Dict] = None, tuples=None) -> None:
        """Constructor"""
        self.filt = {} if filt is None else filt
        self.tuples = [] if tuples is None else tuples
        self.date = datetime.now(pytz.utc)

        if isinstance(filt, str):
            self.filt = {self.filt: None}

        if not isinstance(self.tuples, list):
            self.tuples = [self.tuples]

    # -------------------------------------------
    # PayloadEnvelopes helper messages

    def makePayloadEnvelope(self, result=None, compressionLevel: int = 9):
        from .PayloadEnvelope import PayloadEnvelope

        noMainThread()
        encodedSelf = self.toEncodedPayload(compressionLevel=compressionLevel)
        return PayloadEnvelope(
            self.filt, encodedPayload=encodedSelf, date=self.date, result=result
        )

    @deferToThreadWrapWithLogger(logger)
    def makePayloadEnvelopeDefer(self, result=None, compressionLevel: int = 9):
        return self.makePayloadEnvelope(
            result=result, compressionLevel=compressionLevel
        )

    # -------------------------------------------
    # VortexMsg helper messages

    def makePayloadEnvelopeVortexMsg(
        self, result=None, compressionLevel: int = 9
    ):
        noMainThread()
        return self.makePayloadEnvelope(
            result=result, compressionLevel=compressionLevel
        ).toVortexMsg()

    @deferToThreadWrapWithLogger(logger)
    def makePayloadEnvelopeVortexMsgDefer(
        self, result=None, compressionLevel: int = 9
    ):
        return self.makePayloadEnvelopeVortexMsg(
            result=result, compressionLevel=compressionLevel
        )

    # -------------------------------------------
    # JSON Related methods
    def _fromJson(self, jsonStr):
        jsonDict = ujson.loads(jsonStr)

        assert jsonDict[Jsonable.JSON_CLASS_TYPE] == self.__rapuiSerialiseType__
        return self.fromJsonDict(jsonDict)

    def _toJson(self) -> str:
        return ujson.dumps(self.toJsonDict())

    # -------------------------------------------
    # VortexServer Message Methods
    def toEncodedPayload(self, compressionLevel: int = 9) -> bytes:
        jsonStr = self._toJson()
        return b64encode(
            zlib.compress(jsonStr.encode("UTF-8"), compressionLevel)
        )

    @deferToThreadWrapWithLogger(logger)
    def toEncodedPayloadDefer(self, compressionLevel: int = 9) -> bytes:
        return self.toEncodedPayload(compressionLevel=compressionLevel)

    def fromEncodedPayload(self, encodedPayload: bytes):
        jsonStr = zlib.decompress(b64decode(encodedPayload)).decode()
        return self._fromJson(jsonStr)

    @deferToThreadWrapWithLogger(logger)
    def fromEncodedPayloadDefer(self, encodedPayload: bytes):
        return self.fromEncodedPayload(encodedPayload)

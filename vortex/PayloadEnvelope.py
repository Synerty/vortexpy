"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
import ujson
import zlib
from base64 import b64encode, b64decode
from datetime import datetime
from typing import List, Optional, Dict

import pytz

from .DeferUtil import deferToThreadWrapWithLogger, noMainThread
from .Payload import Payload
from .SerialiseUtil import T_RAPUI_PAYLOAD_ENVELOPE
from .Jsonable import Jsonable

logger = logging.getLogger(__name__)

PayloadEnvelopeList = List["PayloadEnvelope"]
VortexMsgList = List[bytes]


class NoPayloadException(Exception):
    pass


class PayloadEnvelope(Jsonable):
    """Payload
    This object represents a hierarchy of data transferred between client and server
    """

    __fieldNames__ = ["filt", "encodedPayload", "result", "date"]
    __rapuiSerialiseType__ = T_RAPUI_PAYLOAD_ENVELOPE

    vortexUuidKey = "__vortexUuid__"
    vortexNameKey = "__vortexName__"

    def __init__(
        self,
        filt: Optional[Dict] = None,
        encodedPayload: Optional[bytes] = None,
        result=None,
        date: Optional[datetime] = None,
    ) -> None:
        """Constructor"""
        self.filt: Dict = {} if filt is None else filt
        self.encodedPayload: bytes = encodedPayload
        self.result = result
        self.date: datetime = date if date else datetime.now(pytz.utc)

        if isinstance(filt, str):
            self.filt = {self.filt: None}

    # -------------------------------------------
    # PayloadEnvelope Methods

    def isEmpty(self):
        return (
            (
                not self.filt
                or (
                    self.vortexNameKey in self.filt
                    and self.vortexUuidKey in self.filt
                    and len(self.filt) == 2
                )
            )
            and not self.encodedPayload
            and not self.result
        )

    def decodePayload(self) -> Payload:
        if not self.encodedPayload:
            if self.result:
                logger.debug(
                    "encodedPayload is None, but maybe there was an error: %s",
                    self.result,
                )
            raise NoPayloadException()

        noMainThread()
        return Payload().fromEncodedPayload(self.encodedPayload)

    @deferToThreadWrapWithLogger(logger)
    def decodePayloadDefer(self) -> "Payload":
        return self.decodePayload()

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
    def toVortexMsg(self) -> bytes:
        return b64encode(self._toJson().encode())

    @deferToThreadWrapWithLogger(logger)
    def toVortexMsgDefer(self) -> bytes:
        return self.toVortexMsg()

    def fromVortexMsg(self, vortexMsg: bytes) -> "PayloadEnvelope":
        jsonStr = b64decode(vortexMsg).decode()
        return self._fromJson(jsonStr)

    @deferToThreadWrapWithLogger(logger)
    def fromVortexMsgDefer(self, vortexMsg: bytes) -> "PayloadEnvelope":
        return self.fromVortexMsg(vortexMsg)

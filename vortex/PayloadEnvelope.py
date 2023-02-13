"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import base64
import json
import logging
from base64 import b64decode
from base64 import b64encode
from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import pytz

from .DeferUtil import deferToThreadWrapWithLogger
from .DeferUtil import noMainThread
from .Jsonable import Jsonable
from .Payload import Payload
from .SerialiseUtil import T_RAPUI_PAYLOAD_ENVELOPE

logger = logging.getLogger(__name__)

PayloadEnvelopeList = List["PayloadEnvelope"]
VortexMsgList = List[bytes]


class NoPayloadException(Exception):
    pass


class PayloadEnvelope(Jsonable):
    """Payload
    This object represents a hierarchy of data transferred between client and server
    """

    __fieldNames__ = ["filt", "data", "result", "date"]
    __rapuiSerialiseType__ = T_RAPUI_PAYLOAD_ENVELOPE

    vortexUuidKey = "__vortexUuid__"
    vortexNameKey = "__vortexName__"

    def __init__(
        self,
        filt: Optional[Dict] = None,
        encodedPayload: Optional[str] = None,
        result=None,
        date: Optional[datetime] = None,
        data: Optional[Any] = None,
    ) -> None:
        Jsonable.__init__(self)
        """Constructor"""
        assert not (
            data and encodedPayload
        ), "You can not pass encodedData and encodedPayload"

        self.filt: Dict = {} if filt is None else filt
        self.data: Union[Any, str, None] = data if data else encodedPayload
        self.result = result
        self.date: datetime = date if date else datetime.now(pytz.utc)

        if isinstance(filt, str):
            self.filt = {self.filt: None}

    # -------------------------------------------
    # PayloadEnvelope Methods

    @property
    def encodedPayload(self) -> Optional[str]:
        # self.data could be from VortexJS/Typescript where we allow `str` data
        assert self.data is None or isinstance(
            self.data, str
        ), "Encoded payload is not None or a string"
        return self.data

    @encodedPayload.setter
    def encodedPayload(self, val: Optional[str]):
        # This value should only be set from users of VortexPy and
        # Payload.toEncodedPayload so we only allow str or None
        assert val is None or isinstance(
            val, str
        ), "Encoded payload is not None or a string"
        self.data = val

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

    # -------------------------------------------
    # Decode Helper Methods

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
        jsonDict = json.loads(jsonStr)

        assert jsonDict[Jsonable.JSON_CLASS_TYPE] == self.__rapuiSerialiseType__
        return self.fromJsonDict(jsonDict)

    def _toJson(self) -> str:
        return json.dumps(self.toJsonDict())

    # -------------------------------------------
    # VortexServer Message Methods
    def toVortexMsg(self, base64Encode=False) -> bytes:
        jsonStr = self._toJson().encode()
        if base64Encode:
            return b64encode(jsonStr)
        return jsonStr

    @deferToThreadWrapWithLogger(logger)
    def toVortexMsgDefer(self, base64Encode=False) -> bytes:
        return self.toVortexMsg(base64Encode=base64Encode)

    def fromVortexMsg(
        self,
        vortexMsg: bytes,
    ) -> "PayloadEnvelope":
        if vortexMsg.startswith(b"{"):
            jsonStr = vortexMsg.decode()
        else:
            jsonStr = b64decode(vortexMsg).decode()
        return self._fromJson(jsonStr)

    @deferToThreadWrapWithLogger(logger)
    def fromVortexMsgDefer(self, vortexMsg: bytes) -> "PayloadEnvelope":
        return self.fromVortexMsg(vortexMsg)

    @classmethod
    @deferToThreadWrapWithLogger(logger)
    def base64EncodeDefer(cls, vortexMsg) -> bytes:
        return base64.b64encode(vortexMsg)

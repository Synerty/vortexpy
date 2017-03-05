import logging
from datetime import datetime
from typing import Optional
from uuid import uuid1

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class PayloadResponse(Deferred):
    """ Payload Response

    This class is used to catch responses from a sent payload.
    If the remote end is going to send back a payload, with the same filt, this class can
    be used to capture this can and call it's call back.

    If the response is not received within the timeout, the errback is called.

    Here is some example usage.

    ::

        logger = logging.getLogger("Example Payload Response")
        payload = Payload(filt={"rapuiServerEcho":True})
        responseDeferred = PayloadResponse(payload)
        VortexFactory.sendVortexMsg(payload.toVortexMsg())
        logger.info("Payload Sent")

        responseDeferred.addCallbacks(logger.info, logger.error)

    """

    __messageIdKey = "PayloadResponse.messageId"

    PROCESSING = "Processing"
    # NO_ENDPOINT = "No Endpoint"
    FAILED = "Failed"
    SUCCESS = "Success"
    TIMED_OUT = "Timed Out"

    def __init__(self, payload: Payload,
                 destVortexName: Optional[str] = None,
                 destVortexUuid: Optional[str] = None,
                 timeout: int = 10,
                 resultCheck=True,
                 logTimeoutError=True):
        """ Constructor

        Tag and optionally send a payload.

        The timeout starts as soon as the constructor is called.

        :param payload The payload to send to the remote and, and wait for a response for
        :param destVortexName The name of the vortex to send to.
        :param destVortexUuid The UUID of the vortex to send a payload to.
        :param timeout The timeout to wait for a response
        :param resultCheck Should the response payload.result be checked, if it fails
                    it errback will be called.
        """
        Deferred.__init__(self)
        self._resultCheck = resultCheck
        self._logTimeoutError = logTimeoutError

        self._messageId = str(uuid1())
        payload.filt[self.__messageIdKey] = self._messageId

        self._status = self.PROCESSING
        self._date = datetime.utcnow()

        self._endpoint = PayloadEndpoint(payload.filt, self._process)

        # noinspection PyTypeChecker
        self.addTimeout(timeout, reactor)
        self.addErrback(self._timedOut, payload)

        if destVortexName or destVortexUuid:
            VortexFactory.sendVortexMsg(vortexMsgs=payload.toVortexMsg(),
                                        destVortexName=destVortexName,
                                        destVortexUuid=destVortexUuid)

    @classmethod
    def isResponsePayload(cls, payload):
        """ Is Response Payload

        The PayloadResponse tags the payloads, so it expects a unique message back.

        :returns: True if this payload has been tagged by a PayloadResponse class
        """
        return cls.__messageIdKey in payload.filt

    @property
    def status(self):
        return self._status

    def _timedOut(self, failure: Failure, payload: Payload):
        if self._logTimeoutError:
            logger.error("Timed out for payload %s", payload.filt)
        self._status = self.TIMED_OUT
        return failure

    def _process(self, payload, **kwargs):
        if self.called:
            raise Exception("Received response after timeout.")

        if self._resultCheck and not payload.result in (None, True):
            self._status = self.FAILED
            self.errback(Failure(Exception(payload.result)))

        else:
            self._status = self.SUCCESS
            self.callback(payload)

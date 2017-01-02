from datetime import datetime
from uuid import uuid1

from twisted.internet import reactor
from twisted.internet.defer import Deferred

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint


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

    PROCESSING = "Processing"
    # NO_ENDPOINT = "No Endpoint"
    # FAILED = "Failed"
    SUCCESS = "Success"
    TIMED_OUT = "Timed Out"

    def __init__(self, payload: Payload, timeout: int = 10):
        Deferred.__init__(self)

        self._messageId = str(uuid1())
        payload.filt["PayloadResponse.messageId"] = self._messageId

        self._status = self.PROCESSING
        self._date = datetime.utcnow()
        # self._vortexUuid = None
        # self._filt = payload.filt

        self._endpoint = PayloadEndpoint(payload.filt, self._process)

        # noinspection PyTypeChecker
        self.addTimeout(timeout, reactor)
        self.addErrback(self._timedOut)

    @property
    def status(self):
        return self._status

    def _timedOut(self, failure):
        self._status = self.TIMED_OUT
        return failure

    def _process(self, payload, **kwargs):
        if self.called:
            raise Exception("Received response after timeout.")

        self._status = self.SUCCESS
        self.callback(payload)

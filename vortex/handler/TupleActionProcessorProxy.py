import logging
from copy import copy

from twisted.internet.defer import TimeoutError, inlineCallbacks, Deferred
from twisted.python.failure import Failure

from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadResponse import PayloadResponse
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TupleActionProcessorProxy:
    """ Tuple Data Observable Proxy Handler

    This class proxies the TupleActions onto another destination, giving the ability
    to pass through one services into another. EG, from a client facing python service
    to a server backend.
    """

    def __init__(self, tupleActionProcessorName,
                 proxyToVortexName: str,
                 additionalFilt=None) -> None:
        """ Constructor

        :param tupleActionProcessorName: The name of this and the other action handler
        :param proxyToVortexName: The vortex dest name to proxy requests to
        :param additionalFilt: Any additional filter keys that are required
        """
        self._proxyToVortexName = proxyToVortexName

        self._filt = dict(name=tupleActionProcessorName, key="tupleActionProcessorName")

        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)

    def shutdown(self):
        self._endpoint.shutdown()

    @inlineCallbacks
    def _process(self, payloadEnvelope: PayloadEnvelope, vortexName: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):

        # Ignore responses from the backend, these are handled by PayloadResponse
        if vortexName == self._proxyToVortexName:
            return

        # Keep a copy of the incoming filt, in case they are using PayloadResponse
        responseFilt = copy(payloadEnvelope.filt)

        # Track the response, log an error if it fails
        # 5 Seconds is long enough.
        # VortexJS defaults to 10s, so we have some room for round trip time.
        pr = PayloadResponse(
            payloadEnvelope,
            timeout=PayloadResponse.TIMEOUT - 5,  # 5 seconds less
            resultCheck=False,
            logTimeoutError=False
        )

        # This is not a lambda, so that it can have a breakpoint
        def reply(payloadEnvelope: PayloadEnvelope):
            payloadEnvelope.filt = responseFilt
            d: Deferred = payloadEnvelope.toVortexMsgDefer()
            d.addCallback(sendResponse)
            return d

        pr.addCallback(reply)

        pr.addCallback(lambda _: logger.debug("Received action response from server"))
        pr.addErrback(self.__handlePrFailure, payloadEnvelope, sendResponse)

        vortexMsg = yield payloadEnvelope.toVortexMsgDefer()
        try:
            yield VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                              destVortexName=self._proxyToVortexName)
        except Exception as e:
            logger.exception(e)

    @inlineCallbacks
    def __handlePrFailure(self, f: Failure,
                          payloadEnvelope: PayloadEnvelope,
                          sendResponse: SendVortexMsgResponseCallable):
        payload = yield payloadEnvelope.decodePayloadDefer()
        action = payload.tuples[0]
        if f.check(TimeoutError):
            logger.error(
                "Received no response from\nprocessor %s\naction %s",
                self._filt,
                action
            )
        else:
            logger.error(
                "Unexpected error, %s\nprocessor %s\naction %s",
                f,
                self._filt,
                action
            )

        vortexLogFailure(f, logger)

        vortexMsg = yield PayloadEnvelope(filt=payloadEnvelope.filt,
                                          result=str(f.value)).toVortexMsgDefer()

        sendResponse(vortexMsg)

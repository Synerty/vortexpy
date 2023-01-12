import logging
from copy import copy
from typing import Optional
from typing import Union

from twisted.internet.defer import Deferred
from twisted.internet.defer import TimeoutError
from twisted.internet.defer import inlineCallbacks
from twisted.python.failure import Failure

from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadResponse import PayloadResponse
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory
from vortex.VortexUtil import limitConcurrency
from vortex.handler.TupleActionProcessor import TupleActionProcessor
from vortex.handler.TupleActionProcessor import TupleActionProcessorDelegateABC

logger = logging.getLogger(__name__)


class TupleActionProcessorProxy:
    """Tuple Data Observable Proxy Handler

    This class proxies the TupleActions onto another destination, giving the ability
    to pass through one services into another. EG, from a client facing python service
    to a server backend.
    """

    _DEBUG_LOGGING = False

    def __init__(
        self,
        tupleActionProcessorName,
        proxyToVortexName: str,
        additionalFilt=None,
        acceptOnlyFromVortex: Optional[Union[str, tuple]] = None,
    ) -> None:
        """Constructor

        :param tupleActionProcessorName: The name of this and the other action handler
        :param proxyToVortexName: The vortex dest name to proxy requests to
        :param additionalFilt: Any additional filter keys that are required
        """
        self._proxyToVortexName = proxyToVortexName
        self._tupleActionProcessorName = tupleActionProcessorName
        self._delegateProcessor = TupleActionProcessor(
            tupleActionProcessorName=tupleActionProcessorName,
            usedForProxy__=True,
        )

        self._filt = dict(
            name=tupleActionProcessorName, key="tupleActionProcessorName"
        )

        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(
            self._filt,
            self._processLimited,
            acceptOnlyFromVortex=acceptOnlyFromVortex,
        )

    def shutdown(self):
        self._endpoint.shutdown()

    def setDelegate(
        self, tupleName: str, processor: TupleActionProcessorDelegateABC
    ):
        """Add Tuple Action Processor Delegate

        :param tupleName: The tuple name to process actions for.
        :param processor: The processor to use for processing this tuple name.

        """
        self._delegateProcessor.setDelegate(tupleName, processor)

    def _processLimited(self, *args, **kwargs):
        d = self._processLimitedDecorated(*args, **kwargs)
        if d:
            d.addErrback(vortexLogFailure, logger, consumeError=True)

    @limitConcurrency(logger, 20)
    def _processLimitedDecorated(self, *args, **kwargs):
        return self._process(*args, **kwargs)

    @inlineCallbacks
    def _process(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexName: str,
        vortexUuid: str,
        sendResponse: SendVortexMsgResponseCallable,
        **kwargs,
    ) -> None:
        # Ignore responses from the backend, these are handled by PayloadResponse
        if vortexName == self._proxyToVortexName:
            return

        if not VortexFactory.isVortexUuidOnline(vortexUuid):
            logger.debug(
                "Vortex %s is no longer online, skipping reply", vortexUuid
            )
            return

        # logger.debug(
        #     "Vortex %s is STILL online, continuing to reply", vortexUuid
        # )

        # Shortcut the logic, so that we don't decode the payload unless we need to.
        if not self._delegateProcessor.delegateCount:
            yield self._processForProxy(
                payloadEnvelope, vortexName, sendResponse
            )
            return

        # If we have local processors, then work out if this tupleAction is meant for
        # the local processor.
        payload = yield payloadEnvelope.decodePayloadDefer()

        assert (
            len(payload.tuples) == 1
        ), "TupleActionProcessor:%s Expected 1 tuples, received %s" % (
            self._tupleActionProcessorName,
            len(payload.tuples),
        )

        tupleAction = payload.tuples[0]

        if self._delegateProcessor.hasDelegate(tupleAction.tupleName()):
            self._delegateProcessor._processTupleAction(
                payloadEnvelope.filt, sendResponse, tupleAction
            )
            return

        # Else, Just send it on to the delegate we're proxying for (the backend)
        yield self._processForProxy(payloadEnvelope, vortexName, sendResponse)

    @inlineCallbacks
    def _processForProxy(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexName: str,
        sendResponse: SendVortexMsgResponseCallable,
        **kwargs,
    ):
        # Keep a copy of the incoming filt, in case they are using PayloadResponse
        responseFilt = copy(payloadEnvelope.filt)

        # Track the response, log an error if it fails
        # 5 Seconds is long enough.
        # VortexJS defaults to 10s, so we have some room for round trip time.
        pr = PayloadResponse(
            payloadEnvelope,
            timeout=PayloadResponse.TIMEOUT - 5,  # 5 seconds less
            resultCheck=False,
            logTimeoutError=False,
        )

        # This is not a lambda, so that it can have a breakpoint
        def reply(payloadEnvelope: PayloadEnvelope):
            payloadEnvelope.filt = responseFilt
            d: Deferred = payloadEnvelope.toVortexMsgDefer()
            d.addCallback(sendResponse)
            return d

        pr.addCallback(reply)

        if self._DEBUG_LOGGING:
            pr.addCallback(
                lambda _: logger.debug(
                    "Received action response from server for %s",
                    payloadEnvelope.filt,
                )
            )
        pr.addErrback(self.__handlePrFailure, payloadEnvelope, sendResponse)

        vortexMsg = yield payloadEnvelope.toVortexMsgDefer()
        try:
            yield VortexFactory.sendVortexMsg(
                vortexMsgs=vortexMsg, destVortexName=self._proxyToVortexName
            )
        except Exception as e:
            logger.exception(e)

    @inlineCallbacks
    def __handlePrFailure(
        self,
        f: Failure,
        payloadEnvelope: PayloadEnvelope,
        sendResponse: SendVortexMsgResponseCallable,
    ):
        payload = yield payloadEnvelope.decodePayloadDefer()
        action = payload.tuples[0]
        if f.check(TimeoutError):
            logger.error(
                "Received no response from\nprocessor %s\naction %s",
                self._filt,
                action,
            )
        else:
            logger.error(
                "Unexpected error, %s\nprocessor %s\naction %s",
                f,
                self._filt,
                action,
            )

        vortexLogFailure(f, logger)

        vortexMsg = yield PayloadEnvelope(
            filt=payloadEnvelope.filt, result=str(f.value)
        ).toVortexMsgDefer()

        sendResponse(vortexMsg)

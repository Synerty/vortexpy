import logging
from abc import abstractmethod, ABCMeta
from typing import Dict, Optional, Union

from twisted.internet.defer import Deferred, fail, succeed, inlineCallbacks
from twisted.python import failure
from twisted.python.failure import Failure

from vortex.DeferUtil import vortexLogFailure
from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.TupleAction import TupleActionABC
from vortex.VortexABC import SendVortexMsgResponseCallable

logger = logging.getLogger(__name__)


class TupleActionProcessorDelegateABC(metaclass=ABCMeta):
    @abstractmethod
    def processTupleAction(self, tupleAction: TupleActionABC) -> Deferred:
        """Process Tuple Action

        The method generates the vortexMsg for the vortex to send.

        :param tupleAction: The C{TupleAction} to process.

        """


class TupleActionProcessor:
    def __init__(
        self,
        tupleActionProcessorName: str,
        additionalFilt: Optional[Dict] = None,
        defaultDelegate: Optional[TupleActionProcessorDelegateABC] = None,
        acceptOnlyFromVortex: Optional[Union[str, tuple]] = None,
        usedForProxy__=False,
    ) -> None:
        """Constructor

        :param tupleActionProcessorName: The name of this observable

        :param additionalFilt: Any additional filter keys that are required

        :param defaultDelegate: The default delegate to send all actions to

        :param acceptOnlyFromVortex: Accept requests only from this vortex,
            The vortex can be str or tuple of str, or None to accept from any.
        """

        self._tupleActionProcessorName = tupleActionProcessorName
        self._defaultDelegate = defaultDelegate

        self._tupleProcessorsByTupleName: Dict[
            str, TupleActionProcessorDelegateABC
        ] = {}

        if not usedForProxy__:
            self._filt = dict(
                name=tupleActionProcessorName, key="tupleActionProcessorName"
            )

            if additionalFilt:
                self._filt.update(additionalFilt)

            self._endpoint = PayloadEndpoint(
                self._filt,
                self._process,
                acceptOnlyFromVortex=acceptOnlyFromVortex,
            )

    def setDelegate(
        self, tupleName: str, processor: TupleActionProcessorDelegateABC
    ):
        """Add Tuple Action Processor Delegate

        :param tupleName: The tuple name to process actions for.
        :param processor: The processor to use for processing this tuple name.

        """
        assert (
            not tupleName in self._tupleProcessorsByTupleName
        ), "TupleActionProcessor:%s, Tuple name %s is already registered" % (
            self._tupleActionProcessorName,
            tupleName,
        )

        assert isinstance(processor, TupleActionProcessorDelegateABC), (
            "TupleActionProcessor:%s, provider must be an"
            " instance of TupleActionProcessorDelegateABC"
            % self._tupleActionProcessorName
        )

        self._tupleProcessorsByTupleName[tupleName] = processor

    @property
    def delegateCount(self) -> int:
        return len(self._tupleProcessorsByTupleName)

    def hasDelegate(self, tupleName: str) -> bool:
        return tupleName in self._tupleProcessorsByTupleName

    def shutdown(self):
        self._endpoint.shutdown()

    @inlineCallbacks
    def _process(
        self,
        payloadEnvelope: PayloadEnvelope,
        sendResponse: SendVortexMsgResponseCallable,
        **kwargs
    ):
        """Process the Payload / Tuple Action"""

        payload = yield payloadEnvelope.decodePayloadDefer()

        assert (
            len(payload.tuples) == 1
        ), "TupleActionProcessor:%s Expected 1 tuples, received %s" % (
            self._tupleActionProcessorName,
            len(payload.tuples),
        )

        tupleAction = payload.tuples[0]

        self._processTupleAction(
            payloadEnvelope.filt, sendResponse, tupleAction
        )

    def _processTupleAction(
        self, payloadEnvelopeFilt, sendResponse, tupleAction
    ):

        assert isinstance(
            tupleAction, TupleActionABC
        ), "TupleActionProcessor:%s Expected TupleAction, received %s" % (
            self._tupleActionProcessorName,
            tupleAction.__class__,
        )

        tupleName = tupleAction.tupleName()
        processor = self._tupleProcessorsByTupleName.get(tupleName)
        if processor:
            delegate = processor.processTupleAction

        elif self._defaultDelegate:
            delegate = self._defaultDelegate.processTupleAction

        else:
            raise Exception("No delegate registered for %s" % tupleName)
        d = self._customMaybeDeferred(delegate, tupleAction)
        d.addCallback(
            self._callback, payloadEnvelopeFilt, tupleName, sendResponse
        )
        d.addErrback(
            self._errback, payloadEnvelopeFilt, tupleName, sendResponse
        )

    @inlineCallbacks
    def _callback(
        self,
        result,
        replyFilt: dict,
        tupleName: str,
        sendResponse: SendVortexMsgResponseCallable,
    ):

        if not isinstance(result, list):
            result = [result]
        payload = Payload(filt=replyFilt, tuples=result)

        payloadEnvelope = yield payload.makePayloadEnvelopeDefer()
        vortexMsg = yield payloadEnvelope.toVortexMsgDefer()
        try:
            yield sendResponse(vortexMsg)

        except Exception as e:
            logger.error(
                "Failed to send TupleAction response for %s", tupleName
            )
            logger.exception(e)

    @inlineCallbacks
    def _errback(
        self,
        result: Failure,
        replyFilt: dict,
        tupleName: str,
        sendResponse: SendVortexMsgResponseCallable,
    ):

        logger.error(
            "TupleActionProcessor:%s Failed to process TupleActon",
            self._tupleActionProcessorName,
        )

        vortexLogFailure(result, logger)
        failureMessage = result.getErrorMessage()

        payloadEnvelope = PayloadEnvelope(filt=replyFilt, result=failureMessage)
        vortexMsg = yield payloadEnvelope.toVortexMsgDefer()
        try:
            yield sendResponse(vortexMsg)

        except Exception as e:
            logger.error(
                "Failed to send TupleAction response for %s\n%s",
                tupleName,
                failureMessage,
            )
            logger.exception(e)

    def _customMaybeDeferred(self, f, *args, **kw):
        try:
            result = f(*args, **kw)
        except Exception as e:
            return fail(failure.Failure(e))

        if isinstance(result, Deferred):
            return result
        elif isinstance(result, failure.Failure):
            return fail(result)
        else:
            return succeed(result)

import logging
from copy import copy
from typing import List

from rx.subjects import Subject
from twisted.internet.defer import Deferred, inlineCallbacks

from vortex.DeferUtil import vortexLogFailure
from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadResponse import PayloadResponse
from vortex.TupleSelector import TupleSelector
from vortex.VortexFactory import VortexFactory
from vortex.handler.TupleDataObservableCache import TupleDataObservableCache

logger = logging.getLogger(__name__)


class TupleDataObserverClient(TupleDataObservableCache):
    def __init__(
        self,
        destVortexName,
        observableName,
        additionalFilt=None,
        observerName="default",
    ):
        """Constructor

        :param observableName: The name of this observable
        :param additionalFilt: Any additional filter keys that are required
        :param destVortexName: The dest vortex name to send the payloads to

        """
        TupleDataObservableCache.__init__(self)

        self._destVortexName = destVortexName
        self._observableName = observableName
        self._observerName = observerName

        self._sendFilt = dict(
            name=observableName,
            observerName=observerName,
            key="tupleDataObservable",
        )

        self._listenFilt = dict(name=observableName, key="tupleDataObservable")

        if additionalFilt:
            self._sendFilt.update(additionalFilt)
            self._listenFilt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(
            self._listenFilt,
            self._receivePayload,
            acceptOnlyFromVortex=destVortexName,
        )

        # There are no online checks for the vortex
        # isOnlineSub = statusService.isOnline
        # .filter(online= > online == = true)
        # .subscribe(online= > self.vortexOnlineChanged())
        #
        # self.onDestroyEvent.subscribe(() = > isOnlineSub.unsubscribe())
        VortexFactory.subscribeToVortexStatusChange(destVortexName).filter(
            lambda online: online is True
        ).subscribe(self._vortexOnlineChanged)

        TupleDataObservableCache.start(self)

    def shutdown(self):
        self._endpoint.shutdown()
        TupleDataObservableCache.shutdown(self)

    def pollForTuples(
        self, tupleSelector: TupleSelector, logTimeoutError: bool = True
    ) -> Deferred:
        startFilt = copy(self._sendFilt)
        startFilt.update({"subscribe": False, "tupleSelector": tupleSelector})

        def updateCacheCallback(
            payloadEnvelope: PayloadEnvelope,
        ) -> PayloadEnvelope:
            cache, _ = self._updateCache(payloadEnvelope)
            return payloadEnvelope

        pr = PayloadResponse(
            payloadEnvelope=PayloadEnvelope(startFilt),
            destVortexName=self._destVortexName,
            logTimeoutError=logTimeoutError,
        )
        pr.addCallback(updateCacheCallback)
        pr.addCallback(
            lambda payloadEnvelope: payloadEnvelope.decodePayloadDefer()
        )
        pr.addCallback(lambda payload: payload.tuples)
        return pr

    def subscribeToTupleSelector(self, tupleSelector: TupleSelector) -> Subject:
        cache = self._makeCache(tupleSelector)
        self._tellServerWeWantData([tupleSelector])
        return cache.subject

    def _vortexOnlineChanged(self, *args) -> None:
        self._tellServerWeWantData(self._tupleSelectors())

    @inlineCallbacks
    def _receivePayload(self, payloadEnvelope: PayloadEnvelope, **kwargs):
        # If this message is for a specific observer, and it's not us, then discard it.
        filtObserverName = payloadEnvelope.filt.get("observerName")
        if (
            filtObserverName is not None
            and filtObserverName != self._observerName
        ):
            return

        # If this message is an error response, then don't process it.
        if payloadEnvelope.result not in (None, True):
            logger.error(
                "Vortex responded with error : %s" % payloadEnvelope.result
            )
            logger.error(str(payloadEnvelope.filt))
            return

        tupleSelector: TupleSelector = payloadEnvelope.filt["tupleSelector"]

        if not self._hasTupleSelector(tupleSelector):
            return

        cache, requiredUpdate = self._updateCache(payloadEnvelope)
        if not requiredUpdate:
            return

        payload = yield Payload().fromEncodedPayloadDefer(cache.encodedPayload)
        cache.subject.on_next(payload.tuples)

    def _tellServerWeWantData(self, tupleSelectors: List[TupleSelector]):
        for tupleSelector in tupleSelectors:
            self._sendRequestToServer(
                PayloadEnvelope(
                    {"subscribe": True, "tupleSelector": tupleSelector}
                )
            )

    def _sendRequestToServer(self, payload):
        payload.filt.update(self._sendFilt)
        d = VortexFactory.sendVortexMsg(
            vortexMsgs=payload.toVortexMsg(),
            destVortexName=self._destVortexName,
        )
        d.addErrback(vortexLogFailure, logger, consumeError=True)

    def _sendUnsubscribeToServer(self, tupleSelector: TupleSelector):
        payload = PayloadEnvelope()
        payload.filt["tupleSelector"] = tupleSelector
        payload.filt["unsubscribe"] = True
        self._sendRequestToServer(payload)

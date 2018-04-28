import logging
from copy import copy
from typing import List

from rx.subjects import Subject
from twisted.internet.defer import Deferred

from vortex.DeferUtil import vortexLogFailure
from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.TupleSelector import TupleSelector
from vortex.VortexFactory import VortexFactory
from vortex.handler.TupleDataObservableCache import TupleDataObservableCache

logger = logging.getLogger(__name__)


class TupleDataObserverClient(TupleDataObservableCache):
    def __init__(self, destVortexName,
                 observableName,
                 additionalFilt=None,
                 observerName="default"):
        """ Constructor

        :param observableName: The name of this observable
        :param additionalFilt: Any additional filter keys that are required
        :param destVortexName: The dest vortex name to send the payloads to

        """
        TupleDataObservableCache.__init__(self)

        self._destVortexName = destVortexName
        self._observableName = observableName
        self._filt = dict(name=observableName,
                          observerName=observerName,
                          key="tupleDataObservable")

        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._receivePayload)

        # There are no online checks for the vortex
        # isOnlineSub = statusService.isOnline
        # .filter(online= > online == = true)
        # .subscribe(online= > self.vortexOnlineChanged())
        # 
        # self.onDestroyEvent.subscribe(() = > isOnlineSub.unsubscribe())

        TupleDataObservableCache.start(self)

    def shutdown(self):
        self._endpoint.shutdown()
        TupleDataObservableCache.shutdown(self)

    def pollForTuples(self, tupleSelector: TupleSelector) -> Deferred:
        startFilt = copy(self._filt)
        startFilt.update({"subscribe": False,
                          "tupleSelector": tupleSelector})

        def cb(payload: Payload) -> List:
            cache, _ = self._updateCache(payload)
            return cache.tuples

        pr = PayloadResponse(payload=Payload(startFilt),
                             destVortexName=self._destVortexName)
        pr.addCallback(cb)
        return pr

    def subscribeToTupleSelector(self, tupleSelector: TupleSelector) -> Subject:
        cache = self._makeCache(tupleSelector)
        self._tellServerWeWantData([tupleSelector])
        return cache.subject

    # TODO Call this when the other end comes back online
    # IDEA, A subscriber on VortexFactory would be great
    def _vortexOnlineChanged(self) -> None:
        self._tellServerWeWantData(self._tupleSelectors())

    def _receivePayload(self, payload: Payload, **kwargs) -> None:
        if payload.result not in (None, True):
            logger.error("Vortex responded with error : %s" % payload.result)
            logger.error(payload.filt)
            return

        tupleSelector = payload.filt["tupleSelector"]

        if not self._hasTupleSelector(tupleSelector):
            return

        cache, requiredUpdate = self._updateCache(payload)
        if not requiredUpdate:
            return

        cache.subject.on_next(cache.tuples)

    def _tellServerWeWantData(self, tupleSelectors: List[TupleSelector]):
        for tupleSelector in tupleSelectors:
            self._sendRequestToServer(
                Payload({"subscribe": True, "tupleSelector": tupleSelector})
            )

    def _sendRequestToServer(self, payload):
        payload.filt.update(self._filt)
        d = VortexFactory.sendVortexMsg(vortexMsgs=payload.toVortexMsg(),
                                        destVortexName=self._destVortexName)
        d.addErrback(vortexLogFailure, logger, consumeError=True)

    def _sendUnsubscribeToServer(self, tupleSelector: TupleSelector):
        payload = Payload()
        payload.filt["tupleSelector"] = tupleSelector.toJsonStr()
        payload.filt["unsubscribe"] = True
        self._sendRequestToServer(payload)

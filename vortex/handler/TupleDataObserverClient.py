import logging
from copy import copy
from typing import List

from rx.subjects import Subject
from twisted.internet.defer import Deferred

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.TupleSelector import TupleSelector
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TupleDataObserverClient:
    def __init__(self, destVortexName, observableName, additionalFilt=None):
        """ Constructor

        :param observableName: The name of this observable
        :param additionalFilt: Any additional filter keys that are required
        :param destVortexName: The dest vortex name to send the payloads to

        """
        self._destVortexName = destVortexName
        self._observableName = observableName
        self._filt = dict(name=observableName, key="tupleDataObservable")

        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._receivePayload)

        self._subjectsByTupleSelector = {}

        # There are no online checks for the vortex
        # isOnlineSub = statusService.isOnline
        # .filter(online= > online == = true)
        # .subscribe(online= > self.vortexOnlineChanged())
        # 
        # self.onDestroyEvent.subscribe(() = > isOnlineSub.unsubscribe())

    def shutdown(self):
        self._endpoint.shutdown()

    def pollForTuples(self, tupleSelector: TupleSelector) -> Deferred:

        startFilt = copy(self._filt)

        startFilt.update({"subscribe": False,
                          "tupleSelector": tupleSelector
                          })

        pr = PayloadResponse(payload=Payload(startFilt),
                             destVortexName=self._destVortexName)
        pr.addCallback(lambda payload: payload.tuples)
        return pr

    def subscribeToTupleSelector(self, tupleSelector: TupleSelector) -> Subject:

        tsStr = tupleSelector.toJsonStr()
        if tsStr in self._subjectsByTupleSelector:
            return self._subjectsByTupleSelector[tsStr]

        newSubject = Subject()
        self._subjectsByTupleSelector[tsStr] = newSubject
        self._tellServerWeWantData([tupleSelector])

        return newSubject

    # TODO Call this when the other end comes back online
    # IDEA, A subscriber on VortexFactory would be great
    def _vortexOnlineChanged(self) -> None:
        tupleSelectors = []
        for key in self._subjectsByTupleSelector:
            tupleSelectors.append(TupleSelector.fromJsonStr(key))

        self._tellServerWeWantData(tupleSelectors)

    def _receivePayload(self, payload, **kwargs) -> None:
        tupleSelector = payload.filt["tupleSelector"]
        tsStr = tupleSelector.toJsonStr()

        if tsStr not in self._subjectsByTupleSelector:
            return

        subject = self._subjectsByTupleSelector[tsStr]
        subject.on_next(payload.tuples)

    def _tellServerWeWantData(self, tupleSelectors: List[TupleSelector]):
        startFilt = {"subscribe": True}
        startFilt.update(self._filt)

        for tupleSelector in tupleSelectors:
            filt = copy(startFilt)
            filt.update({
                "tupleSelector": tupleSelector
            })

            VortexFactory.sendVortexMsg(vortexMsgs=Payload(filt).toVortexMsg(),
                                        destVortexName=self._destVortexName)

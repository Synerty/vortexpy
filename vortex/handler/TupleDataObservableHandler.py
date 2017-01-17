import logging
from abc import abstractmethod, ABCMeta
from collections import defaultdict

from vortex.Payload import deferToThreadWrap, Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.TupleSelector import TupleSelector
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TuplesProviderABC(metaclass=ABCMeta):
    @abstractmethod
    def makeVortexMsg(self, filt: dict, tupleSelector: TupleSelector) -> bytes:
        """ Make Vortex Msg

        The method generates the vortexMsg for the vortex to send.

        :param filt: The filt for the payload.
        :param tupleSelector: The tuple selector us used to determing which tuples to
        send back,
        """

class TupleDataObservableHandler:
    def __init__(self, observableName, tuplesProvider: TuplesProviderABC,
                 additionalFilt=None, subscriptionsEnabled=True):
        """ Constructor

        :param observableName: The name of this observable

        :param tuplesProvider: The clas that provides us with vortex messages when we
            want a tuple.

        :param additionalFilt: Any additional filter keys that are required

        :param subscriptionsEnabled: Should subscriptions be enabled (default)
        """
        self._subscriptionsEnabled = subscriptionsEnabled
        self._filt = dict(name=observableName,
                          key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)
        self._tuplesProvider = tuplesProvider

        self._vortexUuidsByTupleSelectors = defaultdict(list)

    def shutdown(self):
        self._endpoint.shutdown()

    def _createVortexMsg(self, filt, tupleSelector: TupleSelector) -> bytes:
        vortexMsg = self._tuplesProvider.makeVortexMsg(filt, tupleSelector)
        return vortexMsg

    def _process(self, payload: Payload, vortexUuid: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        tupleSelector = payload.filt["tupleSelector"]

        # Add support for just getting data, no subscription.
        if not "nosub" in payload.filt and self._subscriptionsEnabled:
            self._vortexUuidsByTupleSelectors[tupleSelector.toJsonStr()].append(vortexUuid)

        d = sendResponse(self._createVortexMsg(payload.filt, tupleSelector))
        d.addErrback(lambda f: logger.exception(f.value))

    @deferToThreadWrap
    def notifyOfTupleUpdate(self, tupleSelector: TupleSelector):
        tsStr = tupleSelector.toJsonStr()

        # Get / update the list of observing UUIDs
        observingUuids = self._vortexUuidsByTupleSelectors[tsStr]
        observingUuids = set(observingUuids) & set(VortexFactory.getRemoteVortexUuids())
        self._vortexUuidsByTupleSelectors[tsStr] = list(observingUuids)

        if not observingUuids:
            return

        # Create the vortexMsg
        filt = dict(tupleSelector=tupleSelector)
        filt.update(self._filt)
        vortexMsg = self._createVortexMsg(filt, tupleSelector)

        # Send the vortex messages
        for vortexUuid in observingUuids:
            VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                        destVortexUuid=vortexUuid)

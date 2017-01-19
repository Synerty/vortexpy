import logging
from abc import abstractmethod, ABCMeta
from collections import defaultdict

from vortex.Payload import deferToThreadWrap, Payload, printFailure
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
    def __init__(self, observableName, additionalFilt=None, subscriptionsEnabled=True):
        """ Constructor

        :param observableName: The name of this observable

        :param additionalFilt: Any additional filter keys that are required

        :param subscriptionsEnabled: Should subscriptions be enabled (default)
        """
        self._observableName = observableName
        self._subscriptionsEnabled = subscriptionsEnabled
        self._filt = dict(name=observableName,
                          key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)

        self._vortexUuidsByTupleSelectors = defaultdict(list)

        self._tupleProvidersByTupleName = {}

    def addTupleProvider(self, tupleName, provider: TuplesProviderABC):
        """ Add Tuple Provider

        """
        assert not tupleName in self._tupleProvidersByTupleName, (
            "Observable:%s, Tuple name %s is already registered" %
            (self._observableName, tupleName))

        assert isinstance(provider, TuplesProviderABC), (
            "Observable:%s, provider must be an instance of TuplesProviderABC"
            % self._observableName)

        self._tupleProvidersByTupleName[tupleName] = provider

    def shutdown(self):
        self._endpoint.shutdown()

    def _createVortexMsg(self, filt, tupleSelector: TupleSelector) -> bytes:
        tupleProvider = self._tupleProvidersByTupleName.get(tupleSelector.name)
        assert tupleProvider, (
            "Observable:%s, No providers registered for tupleName %s"
            % (self._observableName, tupleSelector.name))

        vortexMsg = tupleProvider.makeVortexMsg(filt, tupleSelector)
        return vortexMsg

    def _process(self, payload: Payload, vortexUuid: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        tupleSelector = payload.filt["tupleSelector"]

        # Add support for just getting data, no subscription.
        if not "nosub" in payload.filt and self._subscriptionsEnabled:
            self._vortexUuidsByTupleSelectors[tupleSelector.toJsonStr()].append(
                vortexUuid)

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
            d = VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                        destVortexUuid=vortexUuid)
            d.addErrback(printFailure)

import logging
from abc import abstractmethod, ABCMeta
from collections import namedtuple
from typing import Union, Optional, List

from twisted.internet import reactor
from twisted.internet.defer import fail, Deferred, succeed, inlineCallbacks
from twisted.python import failure

from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.TupleSelector import TupleSelector
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TupleSelectorUpdateMapperABC(metaclass=ABCMeta):
    @abstractmethod
    def mapTupleSelector(
        self,
        triggerTupleSelector: TupleSelector,
        allTupleSelectors: List[TupleSelector],
    ) -> List[TupleSelector]:
        """Map Tuple Selector

        Implementing this class provides some intelligence to the Observable
        to trigger a refresh of related data to the triggerTupleSelector.

        :param triggerTupleSelector: The TupleSelector that has triggered the update.
        :param allTupleSelectors: All TupleSelectors being observed from this observable.
        """


class TuplesProviderABC(metaclass=ABCMeta):
    @abstractmethod
    def makeVortexMsg(
        self, filt: dict, tupleSelector: TupleSelector
    ) -> Union[Deferred, bytes]:
        """Make Vortex Msg

        The method generates the vortexMsg for the vortex to send.

        :param filt: The filt for the payload.
        :param tupleSelector: The tuple selector us used to determing which tuples to
        send back,
        """


_ObserverDetails = namedtuple(
    "_ObserverDetails", ["vortexUuid", "observerName"]
)


class _ObserverData:
    def __init__(self, tupleSelector: TupleSelector):
        self.tupleSelector = tupleSelector
        self.observers = set()


class TupleDataObservableHandler:
    def __init__(
        self,
        observableName,
        additionalFilt=None,
        subscriptionsEnabled=True,
        acceptOnlyFromVortex: Optional[Union[str, tuple]] = None,
    ):
        """Constructor

        :param observableName: The name of this observable

        :param additionalFilt: Any additional filter keys that are required

        :param subscriptionsEnabled: Should subscriptions be enabled (default)

        :param acceptOnlyFromVortex: Accept requests only from this vortex,
            The vortex can be str or tuple of str, or None to accept from any.

        """
        self._observableName = observableName
        self._subscriptionsEnabled = subscriptionsEnabled
        self._filt = dict(name=observableName, key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(
            self._filt, self._process, acceptOnlyFromVortex=acceptOnlyFromVortex
        )

        self._observerDataByTupleSelector = {}

        self._tupleProvidersByTupleName = {}
        self._tupleSelectorUpdateMappers = []

    def addTupleProvider(self, tupleName, provider: TuplesProviderABC):
        """Add Tuple Provider"""
        assert (
            not tupleName in self._tupleProvidersByTupleName
        ), "Observable:%s, Tuple name %s is already registered" % (
            self._observableName,
            tupleName,
        )

        assert isinstance(provider, TuplesProviderABC), (
            "Observable:%s, provider must be an instance of TuplesProviderABC"
            % self._observableName
        )

        self._tupleProvidersByTupleName[tupleName] = provider

    def addTupleSelectorUpdateMapper(
        self, mapper: TupleSelectorUpdateMapperABC
    ):
        """Add Tuple Selector Update Mapper

        This mapper will be called every time a tuple selector is notified of an update.

        """
        self._tupleSelectorUpdateMappers.append(mapper)

    def hasTupleProvider(self, tupleName: str):
        return tupleName in self._tupleProvidersByTupleName

    def hasTupleSubscribers(self, tupleSelector: TupleSelector):
        return tupleSelector.toJsonStr() in self._observerDataByTupleSelector

    def shutdown(self):
        self._endpoint.shutdown()
        self._tupleSelectorUpdateMappers = []
        self._tupleProvidersByTupleName = {}
        self._observerDataByTupleSelector = {}

    @inlineCallbacks
    def _createVortexMsg(self, filt, tupleSelector: TupleSelector) -> Deferred:
        tupleProvider = self._tupleProvidersByTupleName.get(tupleSelector.name)
        assert (
            tupleProvider
        ), "Observable:%s, No providers registered for tupleName %s" % (
            self._observableName,
            tupleSelector.name,
        )
        try:
            vortexMsg = yield self._customMaybeDeferred(
                tupleProvider.makeVortexMsg, filt, tupleSelector
            )
        except Exception as e:
            logger.exception(e)
            raise
        if not vortexMsg:
            msg = f"TupleDataProvider did not return VortexPY message {tupleSelector}"
            logger.error(msg)
            raise Exception(msg)
        return vortexMsg

    @inlineCallbacks
    def _process(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexUuid: str,
        sendResponse: SendVortexMsgResponseCallable,
        **kwargs,
    ):
        tupleSelector = payloadEnvelope.filt["tupleSelector"]
        tsStr = tupleSelector.toJsonStr()

        observerDetails = _ObserverDetails(
            vortexUuid, payloadEnvelope.filt.get("observerName")
        )

        # Handle unsubscribe
        if payloadEnvelope.filt.get("unsubscribe"):
            if tsStr in self._observerDataByTupleSelector:
                try:
                    self._observerDataByTupleSelector[tsStr].observers.remove(
                        observerDetails
                    )

                except KeyError:
                    pass

                if not self._observerDataByTupleSelector[tsStr].observers:
                    del self._observerDataByTupleSelector[tsStr]

            return

        # Add support for just getting data, no subscription.
        if self._subscriptionsEnabled and payloadEnvelope.filt.get(
            "subscribe", True
        ):
            if tsStr not in self._observerDataByTupleSelector:
                self._observerDataByTupleSelector[tsStr] = _ObserverData(
                    tupleSelector
                )

            self._observerDataByTupleSelector[tsStr].observers.add(
                observerDetails
            )

        vortexMsg = yield self._createVortexMsg(
            payloadEnvelope.filt, tupleSelector
        )
        try:
            yield sendResponse(vortexMsg)
        except Exception as e:
            logger.exception(e)

    def _getMappedTupleSelectors(
        self, tupleSelector: TupleSelector
    ) -> List[TupleSelector]:

        # Get all tuple selectors
        allTupleSelectors = [
            data.tupleSelector
            for data in self._observerDataByTupleSelector.values()
        ]

        # Create a dict so we end up with only unique ones
        tupleSelectorByStr = {tupleSelector.toJsonStr(): tupleSelector}

        # Run through the mappers
        for mapper in self._tupleSelectorUpdateMappers:
            mappedSelectors = mapper.mapTupleSelector(
                tupleSelector, allTupleSelectors
            )
            if mappedSelectors:
                tupleSelectorByStr.update(
                    {ts.toJsonStr(): ts for ts in mappedSelectors}
                )

        # Return a list of tuple selectors
        return list(tupleSelectorByStr.values())

    def notifyOfTupleUpdate(self, tupleSelector: TupleSelector) -> None:
        """Notify Of Tuple Update

        This method tells the observable that an update has occurred and that it should
        send new data to it's observers.

        Tuple selectors should be identical to the data being observed.

        :param tupleSelector: A tuple selector that describes the scope of the update.
        :returns: None

        """
        tupleSelectors = self._getMappedTupleSelectors(tupleSelector)
        reactor.callLater(0, self._notifyOfTupleUpdateInMain, tupleSelectors)

    def notifyOfTupleUpdateForTuple(self, tupleName: str) -> None:
        """Notify of Tuple Update for Tuple

        Like the above notification method, this method will aquire new data and
        send it to observers.

        .. note:: Calling this will not trigger the TupleSelector Update Mappers as
                  this is an earlier soloution.

                  For all new code, use notifyOfTupleUpdate() and define mappers.

        """
        tupleSelectors = []
        for data in self._observerDataByTupleSelector.values():
            if data.tupleSelector.name == tupleName:
                tupleSelectors.append(data.tupleSelector)

        reactor.callLater(0, self._notifyOfTupleUpdateInMain, tupleSelectors)

    @inlineCallbacks
    def _notifyOfTupleUpdateInMain(self, tupleSelectors: List[TupleSelector]):
        for tupleSelector in tupleSelectors:
            yield self._notifyOfTupleUpdateInMainOne(tupleSelector)

    @inlineCallbacks
    def _notifyOfTupleUpdateInMainOne(self, tupleSelector: TupleSelector):
        tsStr = tupleSelector.toJsonStr()
        if tsStr not in self._observerDataByTupleSelector:
            return

        # Filter out the offline observables
        onlineUuids = set(VortexFactory.getRemoteVortexUuids())
        observers = self._observerDataByTupleSelector[tsStr].observers
        for od in list(observers):
            if od.vortexUuid not in onlineUuids:
                observers.remove(od)

        # Get / update the list of observing UUIDs
        if not observers:
            del self._observerDataByTupleSelector[tsStr]
            return

        # Create the vortexMsg
        filt = dict(tupleSelector=tupleSelector)
        filt.update(self._filt)
        vortexMsg = yield self._createVortexMsg(filt, tupleSelector)

        # We can have multiple Observable clients on the one vortex, so make sure
        # we only send one message for these.
        destVortexUuids = set([od.vortexUuid for od in observers])

        # Send the vortex messages
        for destVortexUuid in destVortexUuids:
            d = VortexFactory.sendVortexMsg(
                vortexMsgs=vortexMsg, destVortexUuid=destVortexUuid
            )
            d.addErrback(vortexLogFailure, logger)

    def _customMaybeDeferred(self, f, *args, **kw):
        try:
            result = f(*args, **kw)
        except Exception as e:
            logger.error(
                "TupleDataObservableHandler:%s TupleProvider failed",
                self._observableName,
            )
            logger.exception(e)
            return fail(failure.Failure(e))

        if isinstance(result, Deferred):
            return result
        elif isinstance(result, failure.Failure):
            return fail(result)
        else:
            return succeed(result)

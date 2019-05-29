import logging
from abc import abstractmethod, ABCMeta
from collections import defaultdict, namedtuple
from typing import Union, Optional

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


class TuplesProviderABC(metaclass=ABCMeta):
    @abstractmethod
    def makeVortexMsg(self, filt: dict,
                      tupleSelector: TupleSelector) -> Union[Deferred, bytes]:
        """ Make Vortex Msg

        The method generates the vortexMsg for the vortex to send.

        :param filt: The filt for the payload.
        :param tupleSelector: The tuple selector us used to determing which tuples to
        send back,
        """


_ObserverDetails = namedtuple("_ObserverDetails", ["vortexUuid", "observerName"])


class TupleDataObservableHandler:
    def __init__(self, observableName,
                 additionalFilt=None,
                 subscriptionsEnabled=True,
                 acceptOnlyFromVortex: Optional[str] = None):
        """ Constructor

        :param observableName: The name of this observable

        :param additionalFilt: Any additional filter keys that are required

        :param subscriptionsEnabled: Should subscriptions be enabled (default)

        :param acceptOnlyFromVortex: Accept requests only from this vortex,
            Or None to accept from any.

        """
        self._observableName = observableName
        self._subscriptionsEnabled = subscriptionsEnabled
        self._filt = dict(name=observableName,
                          key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process,
                                         acceptOnlyFromVortex=acceptOnlyFromVortex)

        self._observerDetailsByTupleSelector = defaultdict(set)

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

    def hasTupleProvider(self, tupleName: str):
        return tupleName in self._tupleProvidersByTupleName

    def hasTupleSubscribers(self, tupleSelector:TupleSelector):
        return tupleSelector.toJsonStr() in self._observerDetailsByTupleSelector

    def shutdown(self):
        self._endpoint.shutdown()

    def _createVortexMsg(self, filt, tupleSelector: TupleSelector) -> Deferred:
        tupleProvider = self._tupleProvidersByTupleName.get(tupleSelector.name)
        assert tupleProvider, (
                "Observable:%s, No providers registered for tupleName %s"
                % (self._observableName, tupleSelector.name))

        vortexMsgDefer = self._customMaybeDeferred(
            tupleProvider.makeVortexMsg, filt, tupleSelector)
        vortexMsgDefer.addErrback(vortexLogFailure, logger)
        return vortexMsgDefer

    @inlineCallbacks
    def _process(self, payloadEnvelope: PayloadEnvelope, vortexUuid: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        tupleSelector = payloadEnvelope.filt["tupleSelector"]
        tsStr = tupleSelector.toJsonStr()

        observerDetails = _ObserverDetails(vortexUuid,
                                           payloadEnvelope.filt.get("observerName"))

        # Handle unsubscribe
        if payloadEnvelope.filt.get("unsubscribe"):
            try:
                self._observerDetailsByTupleSelector[tsStr].remove(observerDetails)
            except KeyError:
                pass

            if not self._observerDetailsByTupleSelector[tsStr]:
                del self._observerDetailsByTupleSelector[tsStr]

            return

        # Add support for just getting data, no subscription.
        if self._subscriptionsEnabled and payloadEnvelope.filt.get("subscribe", True):
            self._observerDetailsByTupleSelector[tsStr].add(observerDetails)

        vortexMsg = yield self._createVortexMsg(payloadEnvelope.filt, tupleSelector)
        try:
            yield sendResponse(vortexMsg)
        except Exception as e:
            logger.exception(e)

    def notifyOfTupleUpdate(self, tupleSelector: TupleSelector) -> None:
        """ Notify Of Tuple Update

        This method tells the observable that an update has occurred and that it should
        send new data to it's observers.

        Tuple selectors should be identical to the data being observed.

        :param tupleSelector: A tuple selector that describes the scope of the update.
        :returns: None

        """
        reactor.callLater(0, self._notifyOfTupleUpdateInMain, tupleSelector)

    def notifyOfTupleUpdateForTuple(self, tupleName: str) -> None:
        """ Notify of Tuple Update for Tuple

        Like the above notification method, this method will aquire new data and
        send it to observers.

        """
        for tupleSelectorStr in self._observerDetailsByTupleSelector:
            tupleSelector = TupleSelector.fromJsonStr(tupleSelectorStr)

            if tupleSelector.name == tupleName:
                self.notifyOfTupleUpdate(tupleSelector)

    @inlineCallbacks
    def _notifyOfTupleUpdateInMain(self, tupleSelector: TupleSelector):
        tsStr = tupleSelector.toJsonStr()

        # Filter out the offline observables
        onlineUuids = set(VortexFactory.getRemoteVortexUuids())
        observerDetails = self._observerDetailsByTupleSelector[tsStr]
        for od in list(observerDetails):
            if od.vortexUuid not in onlineUuids:
                observerDetails.remove(od)

        # Get / update the list of observing UUIDs
        if not observerDetails:
            del self._observerDetailsByTupleSelector[tsStr]
            return

        # Create the vortexMsg
        filt = dict(tupleSelector=tupleSelector)
        filt.update(self._filt)
        vortexMsg = yield self._createVortexMsg(filt, tupleSelector)

        # We can have multiple Observable clients on the one vortex, so make sure
        # we only send one message for these.
        destVortexUuids = set([od.vortexUuid for od in observerDetails])

        # Send the vortex messages
        for destVortexUuid in destVortexUuids:
            d = VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                            destVortexUuid=destVortexUuid)
            d.addErrback(vortexLogFailure, logger)

    def _customMaybeDeferred(self, f, *args, **kw):
        try:
            result = f(*args, **kw)
        except Exception as e:
            logger.error("TupleDataObservableHandler:%s TupleProvider failed",
                         self._observableName)
            logger.exception(e)
            return fail(failure.Failure(e))

        if isinstance(result, Deferred):
            return result
        elif isinstance(result, failure.Failure):
            return fail(result)
        else:
            return succeed(result)

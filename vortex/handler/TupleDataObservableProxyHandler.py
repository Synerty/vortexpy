import logging
from copy import copy

from twisted.internet.defer import TimeoutError, inlineCallbacks
from twisted.python.failure import Failure

from vortex.DeferUtil import deferToThreadWrapWithLogger, vortexLogFailure
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadResponse import PayloadResponse
from vortex.TupleSelector import TupleSelector
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory
from vortex.handler.TupleDataObservableCache import TupleDataObservableCache
from vortex.handler.TupleDataObservableHandler import (
    TupleDataObservableHandler,
    TuplesProviderABC,
)

logger = logging.getLogger(__name__)


class TupleDataObservableProxyHandler(TupleDataObservableCache):
    __CHECK_PERIOD = 30  # seconds

    def __init__(
        self,
        observableName,
        proxyToVortexName: str,
        additionalFilt=None,
        subscriptionsEnabled=True,
        observerName="default",
    ) -> None:
        """Constructor

        :param observableName: The name of this and the other observable
        :param proxyToVortexName: The vortex dest name to proxy requests to
        :param additionalFilt: Any additional filter keys that are required
        :param subscriptionsEnabled: Should subscriptions be enabled (default)
        :param observerName: We can clash with other observers, so where there are
        multiple observers on the one vortex, they should use different names.
        """
        TupleDataObservableCache.__init__(self)

        self._proxyToVortexName = proxyToVortexName
        self._subscriptionsEnabled = subscriptionsEnabled
        self._observerName = observerName
        self._filt = dict(name=observableName, key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        # Create the local observable, this allows local tuple providers
        # The rest are proxied on to the backend
        self._localObservableHandler = TupleDataObservableHandler(
            observableName,
            additionalFilt=additionalFilt,
            subscriptionsEnabled=subscriptionsEnabled,
        )
        # Shutdown the local observables endpoint, we don't want it listening it's self
        self._localObservableHandler.shutdown()

        # Finally, Setup our endpoint
        self._endpoint = PayloadEndpoint(self._filt, self._process)

        TupleDataObservableCache.start(self)

    def shutdown(self):
        self._endpoint.shutdown()
        TupleDataObservableCache.shutdown(self)

    ## ----- Implement local observable

    def addTupleProvider(self, tupleName, provider: TuplesProviderABC):
        """Add Tuple Provider

        Adds a tuple provider to the local observable.

        All other requests are proxied on

        """
        self._localObservableHandler.addTupleProvider(
            tupleName, provider=provider
        )

    def notifyOfTupleUpdate(self, tupleSelector: TupleSelector) -> None:
        """Notify of Tuple Update

        Notifies the local observable that tuples have been updated

        """
        if not self._localObservableHandler.hasTupleProvider(
            tupleSelector.name
        ):
            raise Exception(
                "Local observable doesn't have tuple provider for %s"
                " registered, Proxy is : %s" % (tupleSelector.name, self._filt)
            )

        self._localObservableHandler.notifyOfTupleUpdate(tupleSelector)

    ## ----- Implement proxy from here on in

    @inlineCallbacks
    def _process(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexUuid: str,
        vortexName: str,
        sendResponse: SendVortexMsgResponseCallable,
        **kwargs
    ):
        if vortexName == self._proxyToVortexName:
            yield self._processUpdateFromBackend(payloadEnvelope)

        else:
            yield self._processSubscribeFromFrontend(
                payloadEnvelope, vortexUuid, sendResponse
            )

    def _processSubscribeFromFrontend(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexUuid: str,
        sendResponse: SendVortexMsgResponseCallable,
    ):
        tupleSelector: TupleSelector = payloadEnvelope.filt["tupleSelector"]

        # If the local observable provides this tuple, then use that instead
        if self._localObservableHandler.hasTupleProvider(tupleSelector.name):
            return self._localObservableHandler._process(
                payloadEnvelope=payloadEnvelope,
                vortexUuid=vortexUuid,
                sendResponse=sendResponse,
            )

        # Add support for just getting data, no subscription.
        if payloadEnvelope.filt.get("unsubscribe", True):
            return self._handleUnsubscribe(tupleSelector, vortexUuid)

        elif (
            payloadEnvelope.filt.get("subscribe", True)
            and self._subscriptionsEnabled
        ):
            return self._handleSubscribe(
                payloadEnvelope, tupleSelector, sendResponse, vortexUuid
            )

        else:
            return self._handlePoll(
                payloadEnvelope, tupleSelector, sendResponse
            )

    def _handleUnsubscribe(self, tupleSelector: TupleSelector, vortexUuid: str):

        if not self._hasTupleSelector(tupleSelector):
            return

        cache = self._getCache(tupleSelector)
        try:
            cache.vortexUuids.remove(vortexUuid)
        except KeyError:
            pass

    def _handleSubscribe(
        self,
        payloadEnvelope: PayloadEnvelope,
        tupleSelector: TupleSelector,
        sendResponse: SendVortexMsgResponseCallable,
        vortexUuid: str,
    ):

        # Add support for just getting data, no subscription.
        cache = self._getCache(tupleSelector)
        if (
            cache
            and cache.lastServerPayloadDate is not None
            and cache.cacheEnabled
        ):
            respPayloadEnvelope = PayloadEnvelope(
                filt=payloadEnvelope.filt,
                encodedPayload=cache.encodedPayload,
                date=cache.lastServerPayloadDate,
            )

            d = respPayloadEnvelope.toVortexMsgDefer()
            d.addCallback(sendResponse)
            d.addErrback(vortexLogFailure, logger, consumeError=True)

        elif cache:
            self._sendRequestToServer(payloadEnvelope)

        else:
            cache = self._makeCache(tupleSelector)
            self._sendRequestToServer(payloadEnvelope)

        cache.vortexUuids.add(vortexUuid)
        # Allow the cache to be disabled
        cache.cacheEnabled = (
            cache.cacheEnabled
            and not payloadEnvelope.filt.get("disableCache", False)
        )

    def _sendRequestToServer(self, payloadEnvelope: PayloadEnvelope):
        payloadEnvelope.filt["observerName"] = self._observerName
        d = VortexFactory.sendVortexMsg(
            vortexMsgs=payloadEnvelope.toVortexMsg(),
            destVortexName=self._proxyToVortexName,
        )
        d.addErrback(vortexLogFailure, logger, consumeError=True)

    def _sendUnsubscribeToServer(self, tupleSelector: TupleSelector):
        payloadEnvelope = PayloadEnvelope()
        payloadEnvelope.filt["tupleSelector"] = tupleSelector
        payloadEnvelope.filt["unsubscribe"] = True
        self._sendRequestToServer(payloadEnvelope)

    def _handlePoll(
        self,
        payloadEnvelope: PayloadEnvelope,
        tupleSelector: TupleSelector,
        sendResponse: SendVortexMsgResponseCallable,
    ):

        useCache = payloadEnvelope.filt.get("useCache", True)

        # Keep a copy of the incoming filt, in case they are using PayloadResponse
        responseFilt = copy(payloadEnvelope.filt)

        # Restore the original payload filt (PayloadResponse) and send it back
        def reply(payload):
            payload.filt = responseFilt
            d = payload.toVortexMsgDefer()
            d.addCallback(sendResponse)
            d.addErrback(vortexLogFailure, logger, consumeError=True)
            # logger.debug("Received response from observable")

        if useCache:
            cache = self._getCache(tupleSelector)
            if (
                cache
                and cache.lastServerPayloadDate is not None
                and cache.cacheEnabled
            ):
                payloadEnvelope.encodedPayload = cache.encodedPayload
                payloadEnvelope.date = cache.lastServerPayloadDate
                reply(payloadEnvelope)
                return

        # Track the response, log an error if it fails
        # 5 Seconds is long enough
        pr = PayloadResponse(
            payloadEnvelope,
            timeout=PayloadResponse.TIMEOUT - 5,  # 5 seconds less
            logTimeoutError=False,
        )

        pr.addErrback(self._handlePrFailure, tupleSelector)
        pr.addErrback(vortexLogFailure, logger, consumeError=True)
        pr.addCallback(reply)

        self._sendRequestToServer(payloadEnvelope)

    def _handlePrFailure(self, f: Failure, tupleSelector):
        if f.check(TimeoutError):
            logger.error(
                "Received no response from\nobservable %s\ntuple selector %s",
                self._filt,
                tupleSelector.toJsonStr(),
            )
        else:
            logger.error(
                "Unexpected error, %s\nobservable %s\ntuple selector %s",
                f,
                self._filt,
                tupleSelector.toJsonStr(),
            )

    @deferToThreadWrapWithLogger(logger)
    def _processUpdateFromBackend(self, payloadEnvelope: PayloadEnvelope):

        tupleSelector: TupleSelector = payloadEnvelope.filt["tupleSelector"]

        if not self._hasTupleSelector(tupleSelector):
            return

        cache, requiredUpdate = self._updateCache(payloadEnvelope)
        if not requiredUpdate:
            return

        # Get / update the list of observing UUIDs
        observingUuids = cache.vortexUuids & set(
            VortexFactory.getRemoteVortexUuids()
        )

        if not observingUuids:
            return

        # Create the vortexMsg
        vortexMsg = payloadEnvelope.toVortexMsg()

        # Send the vortex messages
        for vortexUuid in observingUuids:
            d = VortexFactory.sendVortexMsg(
                vortexMsgs=vortexMsg, destVortexUuid=vortexUuid
            )
            d.addErrback(vortexLogFailure, logger, consumeError=True)

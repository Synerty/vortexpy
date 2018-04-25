import logging
from copy import copy
from datetime import datetime, timedelta
from typing import Optional, Dict, Set

import pytz
from twisted.internet import task
from twisted.internet.defer import TimeoutError
from twisted.python.failure import Failure

from vortex.DeferUtil import deferToThreadWrapWithLogger, vortexLogFailure
from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.TupleSelector import TupleSelector
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory
from vortex.handler.TupleDataObservableHandler import TupleDataObservableHandler, \
    TuplesProviderABC

logger = logging.getLogger(__name__)


class _CachedSubscribedData:
    """ Cached Subscribed Data

    The client will now cache
    """
    TEARDOWN_WAIT = 120  # 2 minutes in seconds

    def __init__(self, tupleSelector: TupleSelector, cacheEnabled: bool = True):
        self.tupleSelector: TupleSelector = tupleSelector
        self.vortexUuids: Set[str] = set()
        self.tearDownDate: Optional[datetime] = None
        self.tuples = []
        self.cacheEnabled = True

        #: Last Server Payload Date
        # If the server has responded with a payload, this is the date in the payload
        # @type {Date | null}
        self.lastServerPayloadDate: Optional[datetime] = None;

    def markForTearDown(self) -> None:
        if self.tearDownDate is not None:
            return
        self.tearDownDate = datetime.now(pytz.utc) + timedelta(seconds=self.TEARDOWN_WAIT)

    def resetTearDown(self) -> None:
        self.tearDownDate = None

    def isReadyForTearDown(self) -> bool:
        return (
                self.tearDownDate is not None
                and self.tearDownDate <= datetime.now(pytz.utc)
        )


class TupleDataObservableProxyHandler:
    _CHECK_PERIOD = 30  # seconds

    def __init__(self, observableName, proxyToVortexName: str,
                 additionalFilt=None, subscriptionsEnabled=True,
                 observerName="default"):
        """ Constructor

        :param observableName: The name of this and the other observable
        :param proxyToVortexName: The vortex dest name to proxy requests to
        :param additionalFilt: Any additional filter keys that are required
        :param subscriptionsEnabled: Should subscriptions be enabled (default)
        :param observerName: We can clash with other observers, so where there are
        multiple observers on the one vortex, they should use different names.
        """
        self._proxyToVortexName = proxyToVortexName
        self._subscriptionsEnabled = subscriptionsEnabled
        self._observerName = observerName
        self._filt = dict(name=observableName,
                          key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._cache: Dict[str, _CachedSubscribedData] = {}

        # Create the local observable, this allows local tuple providers
        # The rest are proxied on to the backend
        self._localObservableHandler = TupleDataObservableHandler(
            observableName,
            additionalFilt=additionalFilt,
            subscriptionsEnabled=subscriptionsEnabled)
        # Shutdown the local observables endpoint, we don't want it listening it's self
        self._localObservableHandler.shutdown()

        # Finally, Setup our endpoint
        self._endpoint = PayloadEndpoint(self._filt, self._process)

        self._pollLoopingCall = task.LoopingCall(self._cacheCheck)

        d = self._pollLoopingCall.start(self._CHECK_PERIOD, now=False)
        d.addCallback(vortexLogFailure, logger, consumeError=True)

    def shutdown(self):
        self._endpoint.shutdown()
        self._pollLoopingCall.stop()

    ## ----- Implement local observable

    def _cacheCheck(self):
        for ts, cache in list(self._cache.items()):
            cache.vortexUuids = (
                    cache.vortexUuids & set(VortexFactory.getRemoteVortexUuids())
            )
            if cache.vortexUuids:
                cache.resetTearDown()
            elif cache.isReadyForTearDown():
                self._sendUnsubscribeToServer(cache.tupleSelector)
                del self._cache[ts]
            else:
                cache.markForTearDown()

    def addTupleProvider(self, tupleName, provider: TuplesProviderABC):
        """ Add Tuple Provider

        Adds a tuple provider to the local observable.

        All other requests are proxied on

        """
        self._localObservableHandler.addTupleProvider(tupleName, provider=provider)

    def notifyOfTupleUpdate(self, tupleSelector: TupleSelector) -> None:
        """ Notify of Tuple Update

        Notifies the local observable that tuples have been updated

        """
        if not self._localObservableHandler.hasTupleProvider(tupleSelector.name):
            raise Exception("Local observable doesn't have tuple provider for %s"
                            " registered, Proxy is : %s" % (
                                tupleSelector.name, self._filt
                            ))

        self._localObservableHandler.notifyOfTupleUpdate(tupleSelector)

    ## ----- Implement proxy from here on in

    def _process(self, payload: Payload, vortexUuid: str, vortexName: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        if vortexName == self._proxyToVortexName:
            self._processUpdateFromBackend(payload)
        else:
            self._processSubscribeFromFrontend(payload, vortexUuid, sendResponse)

    def _processSubscribeFromFrontend(self, payload: Payload, vortexUuid: str,
                                      sendResponse: SendVortexMsgResponseCallable):
        tupleSelector: TupleSelector = payload.filt["tupleSelector"]

        # If the local observable provides this tuple, then use that instead
        if self._localObservableHandler.hasTupleProvider(tupleSelector.name):
            return self._localObservableHandler._process(payload=payload,
                                                         vortexUuid=vortexUuid,
                                                         sendResponse=sendResponse)

        # Add support for just getting data, no subscription.
        if payload.filt.get("unsubscribe", True):
            return self._handleUnsubscribe(tupleSelector, vortexUuid)

        elif payload.filt.get("subscribe", True) and self._subscriptionsEnabled:
            return self._handleSubscribe(payload, tupleSelector, sendResponse, vortexUuid)

        else:
            return self._handlePoll(payload, tupleSelector, sendResponse)

    def _handleUnsubscribe(self, tupleSelector: TupleSelector, vortexUuid: str):
        tsStr = tupleSelector.toJsonStr()

        if not tsStr in self._cache:
            return

        cache = self._cache[tsStr]
        try:
            cache.vortexUuids.remove(vortexUuid)
        except KeyError:
            pass

    def _handleSubscribe(self, payload: Payload,
                         tupleSelector: TupleSelector,
                         sendResponse: SendVortexMsgResponseCallable,
                         vortexUuid: str):
        tsStr = tupleSelector.toJsonStr()

        # Add support for just getting data, no subscription.
        cache = self._cache.get(tsStr)
        if cache:
            if cache.lastServerPayloadDate is not None and cache.cacheEnabled:
                payload.tuples = cache.tuples
                d = payload.toVortexMsgDefer()
                d.addCallback(sendResponse)
                d.addErrback(vortexLogFailure, logger, consumeError=True)
                return
        else:
            cache = _CachedSubscribedData(tupleSelector)
            self._cache[tsStr] = cache

        cache.vortexUuids.add(vortexUuid)
        # Allow the cache to be disabled
        cache.cacheEnabled = cache.cacheEnabled and payload.filt.get("cacheEnabled", True)

        self._sendRequestToServer(payload)

    def _sendRequestToServer(self, payload):
        payload.filt["observerName"] = self._observerName
        d = VortexFactory.sendVortexMsg(vortexMsgs=payload.toVortexMsg(),
                                        destVortexName=self._proxyToVortexName)
        d.addErrback(vortexLogFailure, logger, consumeError=True)

    def _sendUnsubscribeToServer(self, tupleSelector: TupleSelector):
        payload = Payload()
        payload.filt["tupleSelector"] = tupleSelector.toJsonStr()
        payload.filt["unsubscribe"] = True
        self._sendRequestToServer(payload)

    def _handlePoll(self, payload: Payload,
                    tupleSelector: TupleSelector,
                    sendResponse: SendVortexMsgResponseCallable):
        tsStr = tupleSelector.toJsonStr()

        # Keep a copy of the incoming filt, in case they are using PayloadResponse
        responseFilt = copy(payload.filt)

        # Restore the original payload filt (PayloadResponse) and send it back
        def reply(payload):
            payload.filt = responseFilt
            d = payload.toVortexMsgDefer()
            d.addCallback(sendResponse)
            d.addErrback(vortexLogFailure, logger, consumeError=True)
            # logger.debug("Received response from observable")

        cache = self._cache.get(tsStr)
        if cache and cache.lastServerPayloadDate is not None and cache.cacheEnabled:
            payload.tuples = cache.tuples
            reply(payload)
            return

        # Track the response, log an error if it fails
        # 5 Seconds is long enough
        pr = PayloadResponse(
            payload,
            timeout=PayloadResponse.TIMEOUT - 5,  # 5 seconds less
            logTimeoutError=False
        )

        pr.addErrback(self._handlePrFailure, tupleSelector)
        pr.addErrback(vortexLogFailure, logger, consumeError=True)
        pr.addCallback(reply)

        self._sendRequestToServer(payload)

    def _handlePrFailure(self, f: Failure, tupleSelector):
        if f.check(TimeoutError):
            logger.error(
                "Received no response from\nobservable %s\ntuple selector %s",
                self._filt,
                tupleSelector.toJsonStr()
            )
        else:
            logger.error(
                "Unexpected error, %s\nobservable %s\ntuple selector %s",
                f,
                self._filt,
                tupleSelector.toJsonStr()
            )

    @deferToThreadWrapWithLogger(logger)
    def _processUpdateFromBackend(self, payload: Payload):
        tupleSelector = payload.filt["tupleSelector"]
        tsStr = tupleSelector.toJsonStr()

        cache = self._cache.get(tsStr)
        if not cache:
            return

        if cache.lastServerPayloadDate is not None:
            # If this is an old payload, then disregard it.
            if payload.date < cache.lastServerPayloadDate:
                return

        cache.lastServerPayloadDate = payload.date
        cache.tuples = payload.tuples

        # Get / update the list of observing UUIDs
        observingUuids = cache.vortexUuids & set(VortexFactory.getRemoteVortexUuids())

        if not observingUuids:
            return

        # Create the vortexMsg
        vortexMsg = payload.toVortexMsg()

        # Send the vortex messages
        for vortexUuid in observingUuids:
            d = VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                            destVortexUuid=vortexUuid)
            d.addErrback(vortexLogFailure, logger, consumeError=True)

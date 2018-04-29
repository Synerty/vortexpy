import logging
import typing
from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Optional, Dict, Set

import pytz
from rx.subjects import Subject
from twisted.internet import task

from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.TupleSelector import TupleSelector
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class _CachedSubscribedData:
    """ Cached Subscribed Data

    The client will now cache
    """
    TEARDOWN_WAIT = 120  # 2 minutes in seconds

    def __init__(self, tupleSelector: TupleSelector, cacheEnabled: bool = True) -> None:
        self.tupleSelector: TupleSelector = tupleSelector
        self.vortexUuids: Set[str] = set()
        self.tearDownDate: Optional[datetime] = None
        self.encodedPayload: bytes = None
        self.cacheEnabled = cacheEnabled

        #: Last Server Payload Date
        # If the server has responded with a payload, this is the date in the payload
        # @type {Date | null}
        self.lastServerPayloadDate: Optional[datetime] = None

        self.subject = Subject()

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


class TupleDataObservableCache:
    __CHECK_PERIOD = 30  # seconds

    def __init__(self) -> None:
        self.__cache: Dict[str, _CachedSubscribedData] = {}

    def shutdown(self):
        self.__cache = {}
        self.__pollLoopingCall.stop()

        for cache in self.__cache.values():
            cache.subject.dispose()

        self.__cache = {}

    def start(self):

        self.__pollLoopingCall = task.LoopingCall(self.__cacheCheck)

        d = self.__pollLoopingCall.start(self.__CHECK_PERIOD, now=False)
        d.addCallback(vortexLogFailure, logger, consumeError=True)

    def _tupleSelectors(self) -> typing.List[TupleSelector]:
        tupleSelectors = []
        for key in self.__cache:
            tupleSelectors.append(TupleSelector()._fromJson(key))
        return tupleSelectors

    ## ----- Implement local observable

    def __cacheCheck(self):
        for ts, cache in list(self.__cache.items()):
            cache.vortexUuids = (
                    cache.vortexUuids & set(VortexFactory.getRemoteVortexUuids())
            )
            if cache.vortexUuids:
                cache.resetTearDown()

            elif cache.isReadyForTearDown():
                self._sendUnsubscribeToServer(cache.tupleSelector)
                del self.__cache[ts]

            else:
                cache.markForTearDown()

    def _getCache(self, tupleSelectorStr: str) -> Optional[_CachedSubscribedData]:
        return self.__cache.get(tupleSelectorStr)

    def _hasTupleSelector(self, tupleSelectorStr: str) -> bool:
        return tupleSelectorStr in self.__cache

    def _makeCache(self, tupleSelector: TupleSelector) -> _CachedSubscribedData:
        tsStr = tupleSelector.toJsonStr()
        if tsStr in self.__cache:
            return self.__cache[tsStr]

        cache = _CachedSubscribedData(tupleSelector)
        self.__cache[tsStr] = cache
        return cache

    def _updateCache(self, payloadEnvelope: PayloadEnvelope) -> typing.Tuple[
        _CachedSubscribedData, bool]:
        """ Update Cache

        Update the cache if it requires it

        :returns a tuple of (cache, requiredUpdate)

        """
        tupleSelector = payloadEnvelope.filt["tupleSelector"]
        tsStr = tupleSelector.toJsonStr()

        cache = self._getCache(tsStr)
        if not cache:
            cache = self._makeCache(tsStr)

        if cache.lastServerPayloadDate is not None:
            if payloadEnvelope.date < cache.lastServerPayloadDate:
                return cache, False

        cache.lastServerPayloadDate = payloadEnvelope.date
        cache.encodedPayload = payloadEnvelope.encodedPayload

        return cache, True

    @abstractmethod
    def _sendUnsubscribeToServer(self, tupleSelector: TupleSelector):
        pass

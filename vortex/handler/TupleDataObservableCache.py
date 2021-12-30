import logging
import typing
import weakref
from abc import abstractmethod, ABCMeta
from collections import defaultdict
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
    """Cached Subscribed Data

    The client will now cache
    """

    TEARDOWN_WAIT = 120  # 2 minutes in seconds

    __memoryLoggingRefs = None
    __memoryLoggingEnabled = False

    @classmethod
    def setupMemoryLogging(cls) -> None:
        cls.__memoryLoggingRefs = []
        cls.__memoryLoggingEnabled = True

    @classmethod
    def memoryLoggingDump(
        cls, top=10, over=100
    ) -> typing.List[typing.Tuple[str, int, int]]:
        if not cls.__memoryLoggingRefs:
            return []

        # Filter out expired items
        cls.__memoryLoggingRefs = list(
            filter(lambda o: o(), cls.__memoryLoggingRefs)
        )

        cachesByTupleType = defaultdict(list)
        for cacheRef in cls.__memoryLoggingRefs:
            cache = cacheRef()
            if not cache:
                continue
            cachesByTupleType[cache.tupleSelector.name].append(
                len(cache.encodedPayload)
            )

        results = []
        for tupleName, lengths in cachesByTupleType.items():
            results.append((tupleName, len(lengths), sum(lengths)))

        data = sorted(results, key=lambda x: x[2], reverse=True)

        return list(filter(lambda x: x[2] >= over, data))[:top]

    def __init__(
        self, tupleSelector: TupleSelector, cacheEnabled: bool = True
    ) -> None:
        if _CachedSubscribedData.__memoryLoggingEnabled:
            _CachedSubscribedData.__memoryLoggingRefs.append(weakref.ref(self))

        self.tupleSelector: TupleSelector = tupleSelector
        self.vortexUuids: Set[str] = set()
        self.tearDownDate: Optional[datetime] = None
        self.encodedPayload: bytes = None

        #: Is the cache enabled
        # HINT: Once it's turned off for a tupleSelector, it remains off.
        # Disabling it can also be done from VortexJS
        self.cacheEnabled = cacheEnabled

        #: Last Server Payload Date
        # If the server has responded with a payload, this is the date in the payload
        # @type {Date | null}
        self.lastServerPayloadDate: Optional[datetime] = None

        self.subject = Subject()

    def markForTearDown(self) -> None:
        if self.tearDownDate is not None:
            return
        self.tearDownDate = datetime.now(pytz.utc) + timedelta(
            seconds=self.TEARDOWN_WAIT
        )

    def resetTearDown(self) -> None:
        self.tearDownDate = None

    def isReadyForTearDown(self) -> bool:
        return (
            self.tearDownDate is not None
            and self.tearDownDate <= datetime.now(pytz.utc)
        )


class TupleDataObservableCache(metaclass=ABCMeta):
    __CHECK_PERIOD = 30  # seconds

    def __init__(self) -> None:
        self.__cache: Dict[str, _CachedSubscribedData] = {}

    def shutdown(self):
        if not self.__pollLoopingCall:
            raise Exception("This has already been shutdown")

        self.__cache = {}
        self.__pollLoopingCall.stop()
        self.__pollLoopingCall = None

        for cache in self.__cache.values():
            cache.subject.dispose()

        self.__cache = {}

    def start(self):

        self.__pollLoopingCall = task.LoopingCall(self.__cacheCheck)

        d = self.__pollLoopingCall.start(self.__CHECK_PERIOD, now=False)
        d.addErrback(vortexLogFailure, logger, consumeError=True)

    def _tupleSelectors(self) -> typing.List[TupleSelector]:
        tupleSelectors = []
        for key in self.__cache:
            tupleSelectors.append(TupleSelector.fromJsonStr(key))
        return tupleSelectors

    ## ----- Implement local observable

    def __cacheCheck(self):
        currentVortexUuids = set(VortexFactory.getRemoteVortexUuids())

        for ts, cache in list(self.__cache.items()):
            cache.vortexUuids = cache.vortexUuids & currentVortexUuids

            if cache.vortexUuids or cache.subject.observers:
                cache.resetTearDown()

            elif cache.isReadyForTearDown():
                logger.debug("Cleaning cache for %s", cache.tupleSelector)
                self._sendUnsubscribeToServer(cache.tupleSelector)
                del self.__cache[ts]

            else:
                cache.markForTearDown()

    def _getCache(
        self, tupleSelector: TupleSelector
    ) -> Optional[_CachedSubscribedData]:
        return self.__cache.get(tupleSelector.toJsonStr())

    def _hasTupleSelector(self, tupleSelectorAny: TupleSelector) -> bool:
        return tupleSelectorAny.toJsonStr() in self.__cache

    def _makeCache(self, tupleSelector: TupleSelector) -> _CachedSubscribedData:
        tsStr = tupleSelector.toJsonStr()
        if tsStr in self.__cache:
            return self.__cache[tsStr]

        cache = _CachedSubscribedData(tupleSelector)
        self.__cache[tsStr] = cache
        return cache

    def _updateCache(
        self, payloadEnvelope: PayloadEnvelope
    ) -> typing.Tuple[_CachedSubscribedData, bool]:
        """Update Cache

        Update the cache if it requires it

        :returns a tuple of (cache, requiredUpdate)

        """
        tupleSelector = payloadEnvelope.filt["tupleSelector"]

        cache = self._getCache(tupleSelector)
        if not cache:
            cache = self._makeCache(tupleSelector)

        if cache.lastServerPayloadDate is not None:
            if payloadEnvelope.date < cache.lastServerPayloadDate:
                return cache, False

        cache.lastServerPayloadDate = payloadEnvelope.date
        cache.encodedPayload = payloadEnvelope.encodedPayload

        return cache, True

    @abstractmethod
    def _sendUnsubscribeToServer(self, tupleSelector: TupleSelector):
        pass

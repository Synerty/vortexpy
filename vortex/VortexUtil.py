import json
from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Set
from typing import Union
from uuid import uuid4

import pytz
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import fail
from twisted.internet.defer import maybeDeferred
from twisted.python.failure import Failure

from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadEnvelope import VortexMsgList
from vortex.Tuple import Tuple
from vortex.Tuple import TupleField
from vortex.Tuple import addTupleType

_LimitConcurrencyArgsTuple = namedtuple(
    "_LimitConcurrencyArgsTuple",
    ("deferred", "funcSelf", "args", "kwargs", "queuedTime"),
)


def _dedupProcessorCallKeys(
    payloadEnvelope: PayloadEnvelope,
    vortexUuid: str,
    *args,
    **kwargs,
):
    filt = payloadEnvelope.filt.copy()
    from vortex.PayloadResponse import PayloadResponse

    filt.pop(PayloadResponse.MESSAGE_ID_KEY, None)
    return vortexUuid, str(filt)


def limitConcurrency(
    decoratorLogger,
    concurrency: int,
    logTaskTimeGreaterThanSeconds=5.0,
    deduplicateBasedOnKwArgsKey: Optional[Callable] = None,
):
    """Limit Concurrency to a call, decorator

    :param decoratorLogger: The logger to log messages for.
    :param concurrency: The number of concurrent calls
    :param logTaskTimeGreaterThanSeconds: If a task takes longer then this
    :param deduplicateBasedOnKwArgsKey: This is a callable that returns a tuple
        of strings. If this argument is provided,
        then duplicate calls with the same values for these arguments will
        be deduplicated. Duplicate calls will use the newest set of arguments.

    # As of python 3.6, dicts are ordered.

    >>> x={}
    >>> x[1]=None
    >>> x
    {1: None}
    >>> x[0]=None
    >>> x
    {1: None, 0: None}
    >>> x[123]=None
    >>> x
    {1: None, 0: None, 123: None}
    # Updating a item in the dict retains the same position
    >>> x[0]=None
    >>> x
    {1: None, 0: None, 123: None}

    # Poping the first off can be done with an iterator.
    >>> next(iter(x.keys()))
    1

    """

    class Wrap:
        def __init__(self, f):
            self._f = f
            self._callQueue: dict[Any, _LimitConcurrencyArgsTuple] = {}
            self._runningCount = 0
            self._processedCount = 0
            self._lastLogTime = None
            self._lastQueueProcessingDelta = None

        def _callComplete(self, result, callStartTime):
            self._runningCount -= 1
            self._processedCount += 1
            # decoratorLogger.debug("Queued task completed %s", str(self._f))

            processingTime = datetime.now(pytz.utc) - callStartTime
            if logTaskTimeGreaterThanSeconds < processingTime.total_seconds():
                decoratorLogger.debug(
                    "%s call took %s",
                    self._f.__name__,
                    processingTime,
                )

            reactor.callLater(0, self._processNext)
            return result

        def _processNext(self):
            now = datetime.now(pytz.utc)
            while self._runningCount < concurrency and self._callQueue:
                # decoratorLogger.debug("Queueing new task %s", str(self._f))
                self._runningCount += 1

                nextKey = next(iter(self._callQueue.keys()))

                args = self._callQueue.pop(nextKey)

                self._lastQueueProcessingDelta = now - args.queuedTime

                callWithArgs = lambda: self._f(
                    args.funcSelf, *args.args, **args.kwargs
                )
                d = maybeDeferred(callWithArgs)
                d.addBoth(self._callComplete, now)
                d.chainDeferred(args.deferred)

            self._logQueueSize()
            self._f.running = self._runningCount
            self._f.waiting = len(self._callQueue)

        def call(self, funcSelf, *args, **kwargs):
            if deduplicateBasedOnKwArgsKey:
                key = deduplicateBasedOnKwArgsKey(*args, **kwargs)
            else:
                key = uuid4()

            if key in self._callQueue:
                decoratorLogger.debug(
                    "%s call was deduplicated for key %s",
                    self._f.__name__,
                    str(key),
                )

            d = Deferred()
            self._callQueue[key] = _LimitConcurrencyArgsTuple(
                d, funcSelf, args, kwargs, datetime.now(pytz.utc)
            )
            self._processNext()
            return d

        def _logQueueSize(self):
            if (
                self._lastLogTime
                and (datetime.now(pytz.utc) - self._lastLogTime).total_seconds()
                < 5.0
            ):
                return

            deltaTime = self._lastQueueProcessingDelta
            queueSize = len(self._callQueue)

            def logIt(logFunc):
                self._lastLogTime = datetime.now(pytz.utc)
                logFunc(
                    "%s last call was queued for %s, queue size %s,"
                    " processed %s since last log message",
                    self._f.__name__,
                    deltaTime,
                    queueSize,
                    self._processedCount,
                )
                self._processedCount = 0

            if 15.0 < deltaTime.total_seconds() or 2000 < queueSize:
                logIt(decoratorLogger.warning)

            elif 5.0 < deltaTime.total_seconds() or 200 < queueSize:
                logIt(decoratorLogger.debug)

    def wrap(f):
        wrapC = Wrap(f)

        def wrapInner(funcSelf, *args, **kwargs):
            return wrapC.call(funcSelf, *args, **kwargs)

        return wrapInner

    return wrap


@addTupleType
class _DebounceArgsTuple(Tuple):
    __tupleType__ = __name__ + "._DebounceArgsTuple"
    args = TupleField()
    kwargs = TupleField()


def debounceCall(debounceSeconds: float):
    """Call a debounced function that delays invoking wrapped function until after wait n seconds

    :param debounceSeconds: cooling down peroid of n seconds
    :return: the wrapped function
    """

    class Wrap:
        _lastCleanup = datetime.now(pytz.utc)
        _cleanupDelta = timedelta(minutes=1, seconds=debounceSeconds)

        _debounceTimeDelta = timedelta(seconds=debounceSeconds)

        _calls: Dict[str, datetime] = {}
        _callsQueued: Set[tuple] = set()

        def __init__(self, f):
            self._f = f

        def __cleanup(self):
            now = datetime.now(pytz.utc)
            # Cleanup the old values
            if self._cleanupDelta < now - self._lastCleanup:
                self._lastCleanup = now

                for key, val in list(self._calls.items()):
                    if (
                        self._cleanupDelta < now - val
                        and key not in self._callsQueued
                    ):
                        self._calls.pop(key)

        def __wrapArgsAndKwargs(self, *args, **kwargs):
            if not args and not kwargs:
                return ""
            hashedArgs = _DebounceArgsTuple(
                args=args, kwargs=kwargs
            ).toJsonDict()
            return json.dumps(hashedArgs)

        def call(self, funcSelf, *args, **kwargs):
            now = datetime.now(pytz.utc)

            # Create a hash of the args
            hashedArgs = self.__wrapArgsAndKwargs(args, kwargs)

            # If this method has never been called before, call it
            lastCallDate = self._calls.get(hashedArgs)
            if not lastCallDate:
                self._calls[hashedArgs] = now
                return self._f(funcSelf, *args, **kwargs)

            # Work out how long it's been since this method has been called
            timeSinceLastCall = now - lastCallDate

            # If this method is overdue for a call, call it
            if timedelta(seconds=debounceSeconds) < timeSinceLastCall:
                # If this call was queued, remove it
                if hashedArgs in self._callsQueued:
                    self._callsQueued.remove(hashedArgs)
                self._calls[hashedArgs] = now
                return self._f(funcSelf, *args, **kwargs)

            timeToNextCall = (
                timedelta(seconds=debounceSeconds) - timeSinceLastCall
            )

            # If not, schedule this method for call after the debounce
            # but only if there isn't already a call queued
            if hashedArgs not in self._callsQueued:
                self._callsQueued.add(hashedArgs)
                reactor.callLater(
                    timeToNextCall.total_seconds() + 0.1,
                    self.call,
                    funcSelf,
                    *args,
                    **kwargs,
                )

            return

    def wrap(f):
        wrapC = Wrap(f)

        def wrapInner(funcSelf, *args, **kwargs):
            return wrapC.call(funcSelf, *args, **kwargs)

        return wrapInner

    return wrap


def logLargeMessages(
    logger, vortexMsgs: Union[VortexMsgList, bytes], destVortexUuid: str
):
    def logIt(vortexMsg):
        # Log anything larger than 128kb
        length = len(vortexMsg)
        if length < 512 * 1024:
            return

        logger.debug(
            "Sending vortexMsg of size %s to vortex %s",
            "{:,}kB".format(int(length / 1024)),
            destVortexUuid,
        )

    if isinstance(vortexMsgs, bytes):
        logIt(vortexMsgs)

    else:
        for msg in vortexMsgs:
            logIt(msg)

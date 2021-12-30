import json
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import Set

import pytz
from twisted.internet import reactor

from vortex.Tuple import TupleField, Tuple, addTupleType


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
                    **kwargs
                )

            return

    def wrap(f):
        wrapC = Wrap(f)

        def wrapInner(funcSelf, *args, **kwargs):
            return wrapC.call(funcSelf, *args, **kwargs)

        return wrapInner

    return wrap

"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import inspect
import logging
import types
import weakref
from copy import copy
from typing import Callable, Union

from vortex.Payload import Payload, VortexMsgList
from vortex.PayloadIO import PayloadIO
from vortex.VortexABC import SendVortexMsgResponseCallable

logger = logging.getLogger(__name__)


class PayloadEndpoint(object):
    """
    The payload endpoint is responsible for matching payloads filters against
    filters defined in the endpoint. If the end point filters are within the
    payload filter then the payload will be passed to the supplied callable.
    """

    def __init__(self, filt, callable_):
        """
        @param filt: The filter to match against payloads
        @param callable_: This will be called and passed the payload if it matches
        """
        if not "key" in filt:
            e = Exception("There is no 'key' in the payload filt"
                          ", There must be one for routing")
            logger.exception(e)
            raise e

        self._wref = None
        if isinstance(callable_, types.FunctionType):
            w = None
            if hasattr(callable_, '_endpointWeakClass'):
                w = callable_._endpointWeakClass

            else:
                class W():
                    def __init__(self, callable_):
                        self._callable = callable_
                        self._callable._endpointWeakClass = self

                    def __call__(self, payload):
                        self._callable(payload)

                w = W(callable_)

            self._wref = weakref.ref(w)

        else:
            weakObject = weakref.ref(callable_.__self__)
            weakMethod = weakref.ref(callable_.__func__)

            def getCallable():
                obj = weakObject()
                func = weakMethod()
                if obj and func:
                    return getattr(obj, func.__name__)
                return None

            self._wref = getCallable

        self._filt = filt
        PayloadIO().add(self)

    @property
    def filt(self):
        return copy(self._filt)

    def check(self, payload):
        def removeUnhashable(filt):
            items = set()
            for key, value in list(filt.items()):
                # We can compare an array of primitives
                if isinstance(value, list):
                    value = tuple(sorted(value))

                # We don't compare complex structures
                if isinstance(value, dict):
                    continue

                if (inspect.isclass(value)
                    and (issubclass(value, dict) or issubclass(value, list))):
                    raise Exception("Class type passed instead of an instance"
                                    " key:%s, value:%s" % (key, value))

                items.add((key, value))
            return items

        theirFilt = removeUnhashable(payload.filt)
        ourFilt = removeUnhashable(self._filt)

        return set(ourFilt).issubset(theirFilt)

    def process(self, payload: Payload,
                vortexUuid: str, vortexName: str, httpSession,
                sendResponse: SendVortexMsgResponseCallable):

        if self.check(payload):
            callable_ = self._wref()
            if callable_:
                return callable_(payload=payload,
                                 vortexUuid=vortexUuid,
                                 vortexName=vortexName,
                                 httpSession=httpSession,
                                 sendResponse=sendResponse)
            else:
                PayloadIO().remove(self)

    def _callableExpired(self, expiredCallable):
        pass
        # PayloadIO().remove(self)

    def __repr__(self):
        callable_ = self._wref()
        if callable_:
            try:
                callbackStr = callable_.__self__.__class__.__name__ + callable_.y.__name__
            except Exception as e:
                callbackStr = str(e)
        else:
            callbackStr = "None"

        s = "Payload id=%s\nfilt=%s\ncallback=%s"
        return s % (id(self), self._filt, callable_)

    def shutdown(self):
        PayloadIO().remove(self)

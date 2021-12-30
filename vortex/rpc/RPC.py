import logging
import sys
from copy import copy
from typing import Optional, Set, Union

from twisted.internet.defer import (
    succeed,
    fail,
    Deferred,
    TimeoutError,
    inlineCallbacks,
)
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure

from vortex.DeferUtil import yesMainThread
from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadPriority import RPC_PRIORITY
from vortex.PayloadResponse import PayloadResponse
from vortex.Tuple import Tuple, addTupleType, TupleField
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


@addTupleType
class _VortexRPCArgTuple(Tuple):
    """Vortex RPC Arg Tuple

    This tuple stores the arguments used to call the remote method.

    """

    __tupleType__ = __name__ + "_VortexRPCArgTuple"

    args = TupleField(defaultValue=[])
    kwargs = TupleField(defaultValue={})


@addTupleType
class _VortexRPCResultTuple(Tuple):
    """Vortex RPC Result Tuple

    This tuple stores the result from the remote procedure call

    """

    __tupleType__ = __name__ + "_VortexRPCResultTuple"

    result = TupleField(defaultValue=None)


class _VortexRPC:
    """Vortex RPC Wrapper Class

    This wrapper class handles the mechanics of listening for the RPC calls (handler)
     and sending calls (PayloadResponse)

    Under the covers, This is what the class.

    #.  The caller will be given a Deferred
    #.  The args, kwargs for Tuples and primitive values will be serialised into payload.
    #.  The payload will be sent across the vortex
    #.  The payload will be deserialized and delivered to a PayloadEndpoint created
            for the decorated method.
    #.  The handler will then call the method.
    #.  The result from the method will then be serialised and sent back to the calling
            vortex.
    #.  The deferred will be called with the result from the remote method.

    """

    __registeredFuncNames: Set[str] = set()

    def __init__(
        self,
        func,
        listeningVortexName: str,
        timeoutSeconds: float,
        acceptOnlyFromVortex: Optional[Union[str, tuple]],
        additionalFilt: dict,
        deferToThread: bool,
        inlineCallbacks: bool,
    ) -> None:
        """

        :param listeningVortexName: If the local vortex name matches this name, then
                a handler will be setup to listen for payloads for this RPC method.

        :param timeoutSeconds: The seconds to wait for a response before calling the
                                deferreds errback with a TimeoutError

        :param acceptOnlyFromVortex: Accept payloads (calls) only from this vortex.
                The vortex can be str or tuple of str, or None to accept from any.

        :param additionalFilt: If specified, the items from this dict will be added
                                to the filt that this RPCs handler listens on.

        :param deferToThread: Should the function be called in a thread, or in the
                        reactors main loop.

        :param inlineCallbacks: Should the function be wrapped in the twisted
                @inlinecallbacks decorator before it's called?.

        """

        self.__func = func
        self.__funcSelf = None
        self.__listeningVortexName = listeningVortexName
        self.__timeoutSeconds = timeoutSeconds
        self.__acceptOnlyFromVortex = acceptOnlyFromVortex
        if isinstance(self.__acceptOnlyFromVortex, str):
            self.__acceptOnlyFromVortex = (self.__acceptOnlyFromVortex,)
        self.__deferToThread = deferToThread
        self.__inlineCallbacks = inlineCallbacks

        self.__funcName = ""
        if func.__globals__["__spec__"]:
            self.__funcName += func.__globals__["__spec__"].name
        self.__funcName += "." + func.__qualname__

        if self.__funcName in self.__registeredFuncNames:
            raise Exception(
                "RPC function name %s is already registered" % self.__funcName
            )
        self.__registeredFuncNames.add(self.__funcName)

        # Define the FILT
        self._filt = {"_internal": "vortexRPC", "key": self.__funcName}
        self._filt.update(additionalFilt)

        # Define the Endpoint

    def start(self, funcSelf=None):
        """Start

        If this is a class method, then bind the function to the
                            object passed in by bindToSelf.

        :param funcSelf: The object to bind the class instance methods self to.

        """
        if VortexFactory.isVortexNameLocal(self.__listeningVortexName):
            self.__ep = PayloadEndpoint(self._filt, self._processCall)
            logger.debug("RPC %s listening", self.__funcName)

        else:
            logger.error(
                "Ignoring request to start listening for RPC %s "
                "as vortex name %s is not local",
                self.__funcName,
                self.__listeningVortexName,
            )

        self.__funcSelf = funcSelf

        return self

    def shutdown(self):
        """Shutdown

        Shuts down the RPC PayloadEndpoint
        """
        self.__ep.shutdown()
        self.__func = None
        self.__funcSelf = None

    @inlineCallbacks
    def _processCall(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexName,
        sendResponse,
        *args,
        **kwargs
    ):
        """Process

        Process the incoming RPC call payloads.

        """
        # If the sending vortex, is local, then ignore it, RPC can not be called locally
        if VortexFactory.isVortexNameLocal(vortexName):
            logger.warning(
                "Received RPC call to %s, from local vortex %s, ignoring it",
                self.__funcName,
                vortexName,
            )
            return

        # Apply the "allow" logic
        if (
            self.__acceptOnlyFromVortex
            and vortexName not in self.__acceptOnlyFromVortex
        ):
            logger.debug(
                "Call from non-accepted vortex %s, allowing only from %s",
                vortexName,
                str(self.__acceptOnlyFromVortex),
            )
            return

        # Get the args tuple
        payload = yield payloadEnvelope.decodePayloadDefer()
        argsTuple = payload.tuples[0]
        assert isinstance(argsTuple, _VortexRPCArgTuple), (
            "argsTuple is not an instance of %s" % _VortexRPCArgTuple
        )

        logger.debug("Received RPC call for %s", self.__funcName)

        # Call the method and setup the callbacks
        result = yield self.callLocally(argsTuple.args, argsTuple.kwargs)
        yield self._processCallCallback(
            result, sendResponse, payloadEnvelope.filt
        )

    @inlineCallbacks
    def _processCallCallback(self, result, sendResponseCallable, filt):

        payloadEnvelope = yield (
            Payload(
                filt=filt, tuples=[_VortexRPCResultTuple(result=result)]
            ).makePayloadEnvelopeDefer()
        )

        vortexMsg = yield payloadEnvelope.toVortexMsgDefer()

        yield sendResponseCallable(vortexMsg, RPC_PRIORITY)

    @inlineCallbacks
    def __call__(self, *args, **kwargs):
        """Call"""
        yesMainThread()

        try:
            # FAKE Exception so we can raise a better stack trace later
            raise Exception()
        except:
            stack = sys.exc_info()[2]

        logger.debug("Calling RPC for %s", self.__funcName)

        payloadEnvelope = yield (
            Payload(
                filt=copy(self._filt),
                tuples=[_VortexRPCArgTuple(args=args, kwargs=kwargs)],
            ).makePayloadEnvelopeDefer(compressionLevel=4)
        )

        pr = PayloadResponse(
            payloadEnvelope,
            timeout=self.__timeoutSeconds,
            resultCheck=False,
            logTimeoutError=False,
            destVortexName=self.__listeningVortexName,
        )

        # Delete the payload, we don't need to keep it in memory while we
        # get the result.
        del payloadEnvelope

        pr.addCallback(self._processResponseCallback, stack)
        pr.addErrback(self._processResponseErrback, stack)

        val = yield pr
        return val

    @inlineCallbacks
    def _processResponseCallback(self, payloadEnvelope: PayloadEnvelope, stack):
        """Process Response Callback

        Convert the PayloadResponse payload to the result from the remotely called
        method.

        """

        if not payloadEnvelope.result in (None, True):
            return Failure(
                Exception(payloadEnvelope.result).with_traceback(stack),
                exc_tb=stack,
            )

        # Get the Result from the payload
        payload = yield payloadEnvelope.decodePayloadDefer()
        resultTuple = payload.tuples[0]
        assert isinstance(resultTuple, _VortexRPCResultTuple), (
            "resultTuple is not an instance of %s" % _VortexRPCResultTuple
        )

        logger.debug("Received RPC result for %s", self.__funcName)

        # Return the remote result
        return resultTuple.result

    def _processResponseErrback(self, failure, stack):
        """Process Response Errback

        Convert the PayloadResponse payload to the result from the remotely called
        method.

        """

        if failure.check(TimeoutError):
            logger.error("Received RPC timeout for %s", self.__funcName)

            return Failure(
                Exception(
                    "RPC call timed out for %s", self.__funcName
                ).with_traceback(stack),
                exc_tb=stack,
            )

        return failure

    def callLocally(self, args, kwargs):
        """Call Locally

        This method calls the wrapped function locally, ensuring it returns a
        deferred as it's result.

        """
        try:
            if self.__funcSelf:
                args = [self.__funcSelf] + args

            if self.__inlineCallbacks:
                result = inlineCallbacks(self.__func)(*args, **kwargs)

            elif self.__deferToThread:
                result = deferToThread(self.__func, *args, **kwargs)

            else:
                result = self.__func(*args, **kwargs)

        except Exception as e:
            return fail(Failure(e))

        if isinstance(result, Deferred):
            return result

        elif isinstance(result, Failure):
            return fail(result)

        else:
            return succeed(result)


def vortexRPC(
    listeningVortexName: str,
    timeoutSeconds: float = 30.0,
    acceptOnlyFromVortex: Optional[Union[str, tuple]] = None,
    additionalFilt: Optional[dict] = None,
    deferToThread: bool = False,
    inlineCallbacks: bool = False,
):
    """Vortex RPC Decorator

    :param listeningVortexName: If the local vortex name matches this name, then
            a handler will be setup to listen for payloads for this RPC method.

    :param timeoutSeconds: The seconds to wait for a response before calling the
                            deferreds errback with a TimeoutError

    :param acceptOnlyFromVortex: Accept payloads (calls) only from this vortex.
            Or None to accept from any.

    :param additionalFilt: If specified, the items from this dict will be added
                            to the filt that this RPCs handler listens on.

    :param deferToThread: Should the function be called in a thread, or in the
                        reactors main loop.

    :param inlineCallbacks: Should the function be wrapped in the twisted
            @inlinecallbacks decorator before it's called?.

    :return A wrapped function, that will now work as an RPC call.


    EXAMPLE:

    Declare the function and decorate it.
    Because this will be imported on both sides of the vortex, we specify the name
    so that it will only be listening on one side ::


            @vortexRPC("listenVortexName")
            def myRemoteAddMethod(arg1:int, kwarg1:int=0) -> int:
                return arg1 + kwarg1


    Call the method

    .. note:: The method will ALWAYS return a deferred, regardless of if the method
                wrapped returns one or not.

    ::


            deferred = myRemoteAddMethod(5, kwarg1=7)
            print(deferred.addCallback(lambda v:print(v)))



    """
    additionalFilt = additionalFilt if additionalFilt else {}

    def decorator(func):
        return _VortexRPC(
            func,
            listeningVortexName,
            timeoutSeconds,
            acceptOnlyFromVortex,
            additionalFilt,
            deferToThread,
            inlineCallbacks,
        )

    return decorator

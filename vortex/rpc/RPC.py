import logging
import sys

from twisted.internet.defer import succeed, fail, Deferred
from twisted.internet.error import TimeoutError
from twisted.python.failure import Failure
from typing import Optional

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.Tuple import Tuple, addTupleType, TupleField
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


@addTupleType
class _VortexRPCArgTuple(Tuple):
    """ Vortex RPC Arg Tuple
    
    This tuple stores the arguments used to call the remote method.
    
    """
    __tupleType__ = __name__ + '_VortexRPCArgTuple'

    args = TupleField(defaultValue=[])
    kwargs = TupleField(defaultValue={})


@addTupleType
class _VortexRPCResultTuple(Tuple):
    """ Vortex RPC Result Tuple
    
    This tuple stores the result from the remote procedure call
    
    """
    __tupleType__ = __name__ + '_VortexRPCResultTuple'

    result = TupleField(defaultValue=None)


class _VortexRPC:
    """ Vortex RPC Wrapper Class
    
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

    def __init__(self, func, listeningVortexName: str,
                 timeoutSeconds: float,
                 acceptOnlyFromVortex: Optional[str],
                 additionalFilt: dict):
        """
    
        :param listeningVortexName: If the local vortex name matches this name, then
                a handler will be setup to listen for payloads for this RPC method.
                
        :param timeoutSeconds: The seconds to wait for a response before calling the 
                                deferreds errback with a TimeoutError
                
        :param acceptOnlyFromVortex: Accept payloads (calls) only from this vortex.
                Or None to accept from any.
                
        :param additionalFilt: If specified, the items from this dict will be added
                                to the filt that this RPCs handler listens on.
        
        """

        self.__func = func
        self.__listeningVortexName = listeningVortexName
        self.__timeoutSeconds = timeoutSeconds
        self.__acceptOnlyFromVortex = acceptOnlyFromVortex

        self._funcName = ''
        if func.__globals__["__spec__"]:
            self._funcName += func.__globals__["__spec__"].name
        self._funcName += "." + func.__qualname__

        # Define the FILT
        self._filt = {
            '_internal': 'vortexRPC',
            'key': self._funcName
        }
        self._filt.update(additionalFilt)

        # Define the Endpoint

    def start(self):
        if VortexFactory.isVortexNameLocal(self.__listeningVortexName):
            self.__ep = PayloadEndpoint(self._filt, self._processCall)
            logger.debug("RPC %s listening", self._funcName)

        else:
            logger.error("Ignoring request to start listening for RPC %s "
                         "as vortex name %s is not local",
                         self._funcName, self.__listeningVortexName)

    def shutdown(self):
        """ Shutdown 
        
        Shuts down the RPC PayloadEndpoint
        """
        self.__ep.shutdown()

    def _processCall(self, payload, vortexName, sendResponse, *args, **kwargs):
        """ Process
        
        Process the incoming RPC call payloads.
        
        """
        # If the sending vortex, is local, then ignore it, RPC can not be called locally
        if VortexFactory.isVortexNameLocal(vortexName):
            logger.warning("Received RPC call to %s, from local vortex %s, ignoring it"
                           , self._funcName, vortexName)
            return

        # Apply the "allow" logic
        if self.__acceptOnlyFromVortex and vortexName != self.__acceptOnlyFromVortex:
            logger.debug("Call from non-accepted vortex %s, allowing only from %s",
                         vortexName, self.__acceptOnlyFromVortex)
            return

        # Get the args tuple
        argsTuple = payload.tuples[0]
        assert isinstance(argsTuple, _VortexRPCArgTuple), (
            "argsTuple is not an instance of %s" % _VortexRPCArgTuple)

        logger.debug("Received RPC call for %s", self._funcName)

        # Call the method and setup the callbacks
        d = self.callLocally(argsTuple.args, argsTuple.kwargs)
        d.addCallback(self._processCallCallback, sendResponse, payload.filt)

        # Allow the normal PayloadIO/PayloadEndpoint handling of exceptions
        return d

    def _processCallCallback(self, result, sendResponseCallable, filt):

        payload = Payload(filt=filt,
                          tuples=[_VortexRPCResultTuple(result=result)])

        sendResponseCallable(payload.toVortexMsg())

    def __call__(self, *args, **kwargs):
        """ Call 
        
        """
        try:
            # FAKE Exception so we can raise a better stack trace later
            raise Exception()
        except:
            stack = sys.exc_info()[2]

        logger.debug("Calling RPC for %s", self._funcName)

        payload = Payload(filt=self._filt,
                          tuples=[_VortexRPCArgTuple(args=args, kwargs=kwargs)])

        pr = PayloadResponse(payload,
                             destVortexName=self.__listeningVortexName,
                             timeout=self.__timeoutSeconds,
                             resultCheck=False,
                             logTimeoutError=False)

        pr.addCallback(self._processResponseCallback, stack)
        pr.addErrback(self._processResponseErrback, stack)

        return pr

    def _processResponseCallback(self, payload, stack):
        """ Process Response Callback
        
        Convert the PayloadResponse payload to the result from the remotely called
        method.
        
        """

        if not payload.result in (None, True):
            return Failure(Exception(payload.result).with_traceback(stack),
                           exc_tb=stack)

        # Get the Result from the payload
        resultTuple = payload.tuples[0]
        assert isinstance(resultTuple, _VortexRPCResultTuple), (
            "resultTuple is not an instance of %s" % _VortexRPCResultTuple)

        logger.debug("Received RPC result for %s", self._funcName)

        # Return the remote result
        return resultTuple.result

    def _processResponseErrback(self, failure, stack):
        """ Process Response Errback

        Convert the PayloadResponse payload to the result from the remotely called
        method.

        """

        if failure.check([TimeoutError]):
            logger.debug("Received RPC timeout for %s", self._funcName)

            return Failure(Exception("RPC call timed out for %s", self._funcName)
                           .with_traceback(stack),
                           exc_tb=stack)

        return failure

    def callLocally(self, args, kwargs):
        """ Call Locally
        
        This method calls the wrapped function locally, ensuring it returns a 
        deferred as it's result.
        
        """
        try:
            result = self.__func(*args, **kwargs)

        except Exception as e:
            return fail(Failure(e))

        if isinstance(result, Deferred):
            return result

        elif isinstance(result, Failure):
            return fail(result)

        else:
            return succeed(result)


def vortexRPC(listeningVortexName: str,
              timeoutSeconds: float = 10.0,
              acceptOnlyFromVortex: Optional[str] = None,
              additionalFilt: Optional[dict] = None):
    """ Vortex RPC Decorator
    
    :param listeningVortexName: If the local vortex name matches this name, then
            a handler will be setup to listen for payloads for this RPC method.
            
    :param timeoutSeconds: The seconds to wait for a response before calling the 
                            deferreds errback with a TimeoutError
            
    :param acceptOnlyFromVortex: Accept payloads (calls) only from this vortex.
            Or None to accept from any.
            
    :param additionalFilt: If specified, the items from this dict will be added
                            to the filt that this RPCs handler listens on.
    
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
        return _VortexRPC(func, listeningVortexName, timeoutSeconds,
                          acceptOnlyFromVortex, additionalFilt)

    return decorator

import logging
from copy import copy

from twisted.internet.defer import TimeoutError
from twisted.python.failure import Failure
from vortex.DeferUtil import vortexLogFailure

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TupleActionProcessorProxy:
    """ Tuple Data Observable Proxy Handler

    This class proxies the TupleActions onto another destination, giving the ability
    to pass through one services into another. EG, from a client facing python service
    to a server backend.
    """

    def __init__(self, tupleActionProcessorName,
                 proxyToVortexName: str, additionalFilt=None):
        """ Constructor

        :param tupleActionProcessorName: The name of this and the other action handler
        :param proxyToVortexName: The vortex dest name to proxy requests to
        :param additionalFilt: Any additional filter keys that are required
        """
        self._proxyToVortexName = proxyToVortexName

        self._filt = dict(name=tupleActionProcessorName, key="tupleActionProcessorName")

        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)

    def shutdown(self):
        self._endpoint.shutdown()

    def _process(self, payload: Payload, vortexName: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):

        # Ignore responses from the backend, these are handled by PayloadResponse
        if vortexName == self._proxyToVortexName:
            return

        # Keep a copy of the incoming filt, in case they are using PayloadResponse
        responseFilt = copy(payload.filt)

        # Track the response, log an error if it fails
        # 5 Seconds is long enouge.
        # VortexJS defaults to 10s, so we have some room for round trip time.
        pr = PayloadResponse(payload, timeout=5, resultCheck=False, logTimeoutError=False)

        # This is not a lambda, so that it can have a breakpoint
        def reply(payload):
            payload.filt = responseFilt
            sendResponse(payload.toVortexMsg())

        def handlePrFailure(f: Failure):
            if f.check(TimeoutError):
                logger.error("Received no response for %s", payload.filt['tupleSelector'])
            else:
                logger.error("Processing exception, %s\n%s", f, payload.tuples)

        pr.addCallback(reply)

        pr.addCallback(lambda _: logger.debug("Received action response from server"))
        pr.addErrback(handlePrFailure)

        d = VortexFactory.sendVortexMsg(vortexMsgs=payload.toVortexMsg(),
                                        destVortexName=self._proxyToVortexName)
        d.addErrback(vortexLogFailure, logger, consumeError=True)

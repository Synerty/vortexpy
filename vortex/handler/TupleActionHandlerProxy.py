import logging

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TupleDataObservableProxyHandler:
    """ Tuple Data Observable Proxy Handler

    This class proxies the TupleActions onto another destination, giving the ability
    to pass through one services into another. EG, from a client facing python service
    to a server backend.
    """

    def __init__(self, tupleActionProcessorName, proxyToVortexName: str,
                 additionalFilt=None):
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

    def _process(self, payload: Payload,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        tupleSelector = payload.filt["tupleSelector"]

        # Track the response, log an error if it fails
        # 5 Seconds is long enouge.
        # VortexJS defaults to 10s, so we have some room for round trip time.
        pr = PayloadResponse(payload, timeout=5)

        # This is not a lambda, so that it can have a breakpoint
        def reply(payload):
            sendResponse(payload.toVortexMsg())

        pr.addCallback(reply)

        pr.addCallback(lambda _: logger.debug("Received action response from server"))
        pr.addErrback(lambda f: logger.error(
            "Received no response, %s\n%s", f, payload.tuples))

        d = VortexFactory.sendVortexMsg(vortexMsgs=payload.toVortexMsg(),
                                        destVortexName=self._proxyToVortexName)
        d.addErrback(lambda f: logger.exception(f.value))

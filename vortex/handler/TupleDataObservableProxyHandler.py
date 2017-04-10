import logging
from collections import defaultdict
from copy import copy

from twisted.python.failure import Failure

from vortex.DeferUtil import deferToThreadWrapWithLogger, vortexLogFailure
from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory
from twisted.internet.defer import TimeoutError

logger = logging.getLogger(__name__)


class TupleDataObservableProxyHandler:
    def __init__(self, observableName, proxyToVortexName: str,
                 additionalFilt=None, subscriptionsEnabled=True):
        """ Constructor

        :param observableName: The name of this and the other observable
        :param proxyToVortexName: The vortex dest name to proxy requests to
        :param additionalFilt: Any additional filter keys that are required
        :param subscriptionsEnabled: Should subscriptions be enabled (default)
        """
        self._proxyToVortexName = proxyToVortexName
        self._subscriptionsEnabled = subscriptionsEnabled
        self._filt = dict(name=observableName,
                          key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)

        self._vortexUuidsByTupleSelectors = defaultdict(list)

    def shutdown(self):
        self._endpoint.shutdown()

    def _process(self, payload: Payload, vortexUuid: str, vortexName: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        if vortexName == self._proxyToVortexName:
            self._processUpdateFromBackend(payload)
        else:
            self._processSubscribeFromFrontend(payload, vortexUuid, sendResponse)

    def _processSubscribeFromFrontend(self, payload: Payload, vortexUuid: str,
                                      sendResponse: SendVortexMsgResponseCallable):
        tupleSelector = payload.filt["tupleSelector"]

        # Keep a copy of the incoming filt, in case they are using PayloadResponse
        responseFilt = copy(payload.filt)

        # Track the response, log an error if it fails
        # 5 Seconds is long enough
        pr = PayloadResponse(payload, timeout=5, logTimeoutError=False)

        # Add support for just getting data, no subscription.
        if payload.filt.get("subscribe", True) and self._subscriptionsEnabled:
            self._vortexUuidsByTupleSelectors[tupleSelector.toJsonStr()].append(
                vortexUuid)
        else:
            # Restore the original payload filt (PayloadResponse) and send it back
            def reply(payload):
                payload.filt = responseFilt
                d = sendResponse(payload.toVortexMsg())
                d.addErrback(vortexLogFailure, logger, consumeError=True)
                # logger.debug("Received response from observable")

            pr.addCallback(reply)

        def handlePrFailure(f: Failure):
            if f.check(TimeoutError):
                logger.error("Received no response from observable %s", tupleSelector)
            else:
                logger.error("Unexpected error, %s\n%s", f, tupleSelector)

        pr.addErrback(vortexLogFailure, logger, consumeError=True)

        d = VortexFactory.sendVortexMsg(vortexMsgs=payload.toVortexMsg(),
                                        destVortexName=self._proxyToVortexName)
        d.addErrback(vortexLogFailure, logger, consumeError=True)

    @deferToThreadWrapWithLogger(logger)
    def _processUpdateFromBackend(self, payload: Payload):
        tupleSelector = payload.filt["tupleSelector"]

        tsStr = tupleSelector.toJsonStr()

        # Get / update the list of observing UUIDs
        observingUuids = self._vortexUuidsByTupleSelectors[tsStr]
        observingUuids = set(observingUuids) & set(VortexFactory.getRemoteVortexUuids())
        self._vortexUuidsByTupleSelectors[tsStr] = list(observingUuids)

        if not observingUuids:
            return

        # Create the vortexMsg
        vortexMsg = payload.toVortexMsg()

        # Send the vortex messages
        for vortexUuid in observingUuids:
            d = VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                        destVortexUuid=vortexUuid)
            d.addErrback(vortexLogFailure, logger, consumeError=True)

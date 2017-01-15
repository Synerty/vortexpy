import logging
from collections import defaultdict

from vortex.Payload import Payload, deferToThreadWrap
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadResponse import PayloadResponse
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TupleDataObservableProxy:
    def __init__(self, observableName, proxyToVortexName: str, additionalFilt=None):
        self._proxyToVortexName = proxyToVortexName
        self._filt = dict(name=observableName,
                          key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)

        self._vortexUuidsByTupleSelectors = defaultdict(list)

    def shutdown(self):
        self._endpoint.shutdown()


    def _process(self, payload: Payload, vortexUuid: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        if payload.tuples:
            self._processSubscribeFromFrontend(payload, vortexUuid)
        else:
            self._processUpdateFromBackend(payload)

    def _processSubscribeFromFrontend(self, payload: Payload, vortexUuid: str):
        tupleSelector = payload.filt["tupleSelector"]

        self._vortexUuidsByTupleSelectors[tupleSelector.toJsonStr()].append(vortexUuid)

        # Track the response, log an error if it fails
        pr = PayloadResponse(payload, timeout=15)
        pr.addCallback(lambda _:logger.debug("Received response from observable"))
        pr.addErrback(lambda f: logger.exception(f.value))

        d = VortexFactory.sendVortexMsg(vortexMsgs=payload.toVortexMsg(),
                                        destVortexName=self._proxyToVortexName)
        d.addErrback(lambda f: logger.exception(f.value))

    @deferToThreadWrap
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
            VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                        destVortexUuid=vortexUuid)

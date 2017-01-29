import logging

from copy import copy
from twisted.internet.defer import Deferred

from vortex.Payload import Payload
from vortex.PayloadResponse import PayloadResponse
from vortex.TupleAction import TupleActionABC

logger = logging.getLogger(__name__)


class TupleDataActionClient:
    def __init__(self, destVortexName: str,
                 tupleActionProcessorName: str, additionalFilt: dict = None):
        """ Constructor

        :param destVortexName: The name of the destination vortex to send to.

        :param tupleActionProcessorName: The name of this observable

        :param additionalFilt: Any additional filter keys that are required
        """
        self._destVortexName = destVortexName
        self._filt = dict(name=tupleActionProcessorName,
                          key="tupleActionProcessorName")
        if additionalFilt:
            self._filt.update(additionalFilt)

    def pushAction(self, tupleAction: TupleActionABC, additionalFilt=None) -> Deferred:
        """ Push Action

        This pushes the action, either locally or to the server, depending on the
        implementation.

        If pushed locally, the promise will resolve when the action has been saved.
        If pushed directly to the server, the promise will resolve when the server has
        responded.

        :param tupleAction The tuple action to send to the remote end
        :param additionalFilt: Any extra filt that may be required to send with the
                payload.
        """
        filt = copy(self._filt)
        if additionalFilt:
            filt.update(additionalFilt)

        payload = Payload(filt=filt, tuples=[tupleAction])
        payloadResponse = PayloadResponse(payload, destVortexName=self._destVortexName)

        # Convert the data to TupleAction
        payloadResponse.addCallback(lambda payload: payload.tuples[0])
        return payloadResponse

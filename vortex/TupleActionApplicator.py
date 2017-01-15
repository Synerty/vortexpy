from rx.subjects.subject import Subject

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.VortexABC import SendVortexMsgResponseCallable


class TupleActionApplicator:
    def __init__(self, applicatorName:str,
                 additionalFilt=None):

        self._filt = dict(name=applicatorName,
                          key="tupleActionApplicator")

        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)
        self._subject = Subject()

    def subscribe(self, on_next=None, on_error=None, on_completed=None, observer=None):
        return self._subject.subscribe(on_next, on_error, on_completed, observer)

    def _process(self, payload: Payload, vortexUuid: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        pass



import logging
from abc import abstractmethod, ABCMeta
from collections import defaultdict

from vortex.Payload import deferToThreadWrap, Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.Tuple import tupleForTupleName
from vortex.TupleSelector import TupleSelector
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class TuplesProviderABC(metaclass=ABCMeta):
    @abstractmethod
    def makeVortexMsg(self, filt: dict, tupleSelector: TupleSelector) -> bytes:
        """ Make Vortex Msg

        The method generates the vortexMsg for the vortex to send.

        :param filt: The filt for the payload.
        :param tupleSelector: The tuple selector us used to determing which tuples to
        send back,
        """


class TuplesProviderForDB(TuplesProviderABC):
    def __init__(self, ormSessionCreatorFunc):
        self._ormSessionCreatorFunc = ormSessionCreatorFunc

    def makeVortexMsg(self, filt: dict, tupleSelector: TupleSelector) -> bytes:
        """ Make VortexMsg for DB

        Considerations for this method.

        #.  It must return a vortexMsg (bytes), this ensures all the database access
                and lazy loading is completed in this thread, in this session.

        #.  It must ensure the session is closed on/after exit.

        """

        TupleClass = tupleForTupleName(tupleSelector.name)

        with self._ormSessionCreatorFunc() as ormSession:
            qry = ormSession.query(TupleClass)
            for key, value in tupleSelector.selector.items():
                qry = qry.filter(**{getattr(TupleClass, key): value})

            return Payload(filt=filt, tuples=qry.all()).toVortexMsg()


class TupleDataObservableHandler:
    def __init__(self, observableName, tuplesProvider: TuplesProviderABC,
                 additionalFilt=None):
        self._filt = dict(name=observableName,
                          key="tupleDataObservable")
        if additionalFilt:
            self._filt.update(additionalFilt)

        self._endpoint = PayloadEndpoint(self._filt, self._process)
        self._tuplesProvider = tuplesProvider

        self._vortexUuidsByTupleSelectors = defaultdict(list)

    def shutdown(self):
        self._endpoint.shutdown()

    def _createVortexMsg(self, tupleSelector: TupleSelector) -> bytes:
        filt = dict(tupleSelector=tupleSelector)
        filt.update(self._filt)
        vortexMsg = self._tuplesProvider.makeVortexMsg(filt, tupleSelector)
        return vortexMsg

    def _process(self, payload: Payload, vortexUuid: str,
                 sendResponse: SendVortexMsgResponseCallable, **kwargs):
        tupleSelector = payload.filt["tupleSelector"]

        self._vortexUuidsByTupleSelectors[tupleSelector.toJsonStr()].append(vortexUuid)

        d = sendResponse(self._createVortexMsg(tupleSelector))
        d.addErrback(lambda f: logger.exception(f.value))

    @deferToThreadWrap
    def notifyOfTupleUpdate(self, tupleSelector: TupleSelector):
        tsStr = tupleSelector.toJsonStr()

        # Get / update the list of observing UUIDs
        observingUuids = self._vortexUuidsByTupleSelectors[tsStr]
        observingUuids = set(observingUuids) & set(VortexFactory.getRemoteVortexUuids())
        self._vortexUuidsByTupleSelectors[tsStr] = list(observingUuids)

        if not observingUuids:
            return

        # Create the vortexMsg
        vortexMsg = self._createVortexMsg(tupleSelector)

        # Send the vortex messages
        for vortexUuid in observingUuids:
            VortexFactory.sendVortexMsg(vortexMsgs=vortexMsg,
                                        destVortexUuid=vortexUuid)

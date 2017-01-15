from vortex.Payload import Payload
from vortex.Tuple import tupleForTupleName
from vortex.TupleSelector import TupleSelector
from vortex.handler.TupleDataObservableHandler import TuplesProviderABC


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


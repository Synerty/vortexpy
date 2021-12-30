"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from copy import copy
from typing import Callable, List
from typing import Union

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from twisted.internet.defer import inlineCallbacks
from twisted.internet.threads import deferToThread

from vortex.DeferUtil import deferToThreadWrapWithLogger
from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import VortexMsgList, PayloadEnvelope
from vortex.PayloadFilterKeys import plIdKey, plDeleteKey
from vortex.Tuple import Tuple
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class OrmCrudHandlerExtension(object):
    def uiData(self, tuple_, tuples, session, payloadFilt):
        return True

    def afterCreate(self, tuple_, tuples, session, payloadFilt):
        return True

    def afterRetrieve(self, tuple_, tuples, session, payloadFilt):
        return True

    def beforeUpdate(self, tuple_, tuples, session, payloadFilt):
        return True

    def middleUpdate(self, tuple_, tuples, session, payloadFilt):
        return True

    def afterUpdate(self, tuple_, tuples, session, payloadFilt):
        return True

    def afterUpdateCommit(self, tuple_, tuples, session, payloadFilt):
        return True

    def beforeDelete(self, tuple_, tuples, session, payloadFilt):
        return True

    def afterDeleteCommit(self, tuple_, tuples, session, payloadFilt):
        return True


class _OrmCrudExtensionProcessor(object):
    def __init__(self):
        self.extensions = {}

    def addExtensionClassDecorator(self, Tuple):
        def f(cls):
            self.extensions[Tuple.tupleType()] = cls()
            return cls

        return f

    def addExtensionObject(self, Tuple, ormCrudHandlerExtension):
        self.extensions[Tuple.tupleType()] = ormCrudHandlerExtension

    def uiData(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            if not hasattr(tuple_, "uiData"):
                continue
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.uiData(tuple_, tuples, session, payloadFilt)

    def afterCreate(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.afterCreate(tuple_, tuples, session, payloadFilt)

        self.uiData(tuples, session, payloadFilt)

    def afterRetrieve(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.afterRetrieve(tuple_, tuples, session, payloadFilt)

        self.uiData(tuples, session, payloadFilt)

    def beforeUpdate(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.beforeUpdate(tuple_, tuples, session, payloadFilt)

    def middleUpdate(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.middleUpdate(tuple_, tuples, session, payloadFilt)

    def afterUpdate(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.afterUpdate(tuple_, tuples, session, payloadFilt)

    def afterUpdateCommit(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.afterUpdateCommit(
                    tuple_, tuples, session, payloadFilt
                )

        self.uiData(tuples, session, payloadFilt)

    def beforeDelete(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.beforeDelete(tuple_, tuples, session, payloadFilt)

    def afterDeleteCommit(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.afterDeleteCommit(
                    tuple_, tuples, session, payloadFilt
                )


class OrmCrudHandler(object):
    UPDATE = 1
    CREATE = 2
    DELETE = 3
    QUERY = 4

    def __init__(
        self, sessionFunctor, Declarative, payloadFilter, retreiveAll=False
    ):
        """Create CRUD Hanlder

        This handler will perform crud operations for an SQLAlchemy Declarative

        """
        self._sessionFunctor = sessionFunctor
        self._Declarative = Declarative
        self._payloadFilter = (
            payloadFilter
            if isinstance(payloadFilter, dict)
            else {"key": payloadFilter}
        )

        self._ep = PayloadEndpoint(self._payloadFilter, self._process)

        self._ext = _OrmCrudExtensionProcessor()

        self._retreiveAll = retreiveAll

    def shutdown(self):
        self._ep.shutdown()

    def addExtension(self, Tuple, ormCrudHandlerExtension=None):
        if ormCrudHandlerExtension:
            self._ext.addExtensionObject(Tuple, ormCrudHandlerExtension)
            return
        return self._ext.addExtensionClassDecorator(Tuple)

    @inlineCallbacks
    def _process(self, *args, **kwargs):
        val = yield self._processInThread(*args, **kwargs)
        return val

    @deferToThreadWrapWithLogger(logger)
    def _processInThread(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexUuid: str,
        sendResponse: Callable[[Union[VortexMsgList, bytes]], None],
        **kwargs
    ):

        # Execute preprocess functions
        if self.preProcess(payloadEnvelope, vortexUuid, **kwargs) != None:
            return

        # Create reply payload replyFilt

        replyFilt = copy(self._payloadFilter)
        if payloadEnvelope.filt:
            replyFilt.update(payloadEnvelope.filt)

        # Get data from the payload
        phId = payloadEnvelope.filt.get(plIdKey)
        delete = payloadEnvelope.filt.get(plDeleteKey, False)

        # Setup variables to populate
        replyPayloadEnvelope: PayloadEnvelope = None
        action = None

        tuples = []
        if payloadEnvelope.encodedPayload:
            tuples = payloadEnvelope.decodePayload().tuples

        session = self._getSession()

        # Execute the action
        try:
            if delete == True:
                action = self.DELETE
                replyPayloadEnvelope = self._delete(
                    session, tuples, phId, payloadEnvelope.filt
                )

            elif len(tuples):
                action = self.UPDATE
                replyPayloadEnvelope = self._update(
                    session, tuples, payloadEnvelope.filt
                )

            elif phId != None:
                action = self.QUERY
                replyPayloadEnvelope = self._retrieve(
                    session, phId, payloadEnvelope.filt
                )

            elif len(tuples) == 0:
                action = self.CREATE
                replyPayloadEnvelope = self._create(
                    session, payloadEnvelope.filt
                )

            else:
                session.close()
                raise Exception("Invalid ORM CRUD parameter state")

        except Exception as e:
            replyPayloadEnvelope = PayloadEnvelope(
                result=str(e), filt=replyFilt
            )
            sendResponse(replyPayloadEnvelope.toVortexMsg())
            try:
                session.rollback()
            except:
                pass
            session.close()
            raise

        # Prefer reply filt, if not combine our accpt filt with the filt we were sent

        # Ensure any delegates are playing nice with the result
        if (
            action in (self.DELETE, self.UPDATE)
            and replyPayloadEnvelope.result is None
        ):
            replyPayloadEnvelope.result = True

        replyPayloadEnvelope.filt = replyFilt
        sendResponse(replyPayloadEnvelope.toVortexMsg())

        # Execute the post process function
        self.postProcess(action, payloadEnvelope.filt, vortexUuid)
        session.commit()
        session.close()

    def _getSession(self):
        if isinstance(self._sessionFunctor, Session):
            return self._sessionFunctor

        return self._sessionFunctor()

    def _getDeclarativeById(self, session, id_):
        qry = session.query(self._Declarative)
        if self._retreiveAll and id_ is None:
            return qry.all()

        try:
            return qry.filter(self._Declarative.id == id_).one()
        except NoResultFound as e:
            return None

    def createDeclarative(self, session, payloadFilt):
        if self._retreiveAll:
            return session.query(self._Declarative).all()

        return [self._Declarative()]

    def _getDeclarativeByTuple(self, session, tuple_):
        T = tuple_.__class__
        return session.query(T).filter(T.id == tuple_.id).one()

    def _create(self, session, payloadFilt) -> PayloadEnvelope:
        tuples = self.createDeclarative(session, payloadFilt)
        payload = Payload(tuples=tuples)
        self._ext.afterCreate(payload.tuples, session, payloadFilt)
        return payload.makePayloadEnvelope()

    def _retrieve(
        self, session, filtId, payloadFilt, obj=None, **kwargs
    ) -> PayloadEnvelope:
        ph = obj if obj else self._getDeclarativeById(session, filtId)
        payload = Payload()
        payload.tuples = [ph] if ph else []
        self._ext.afterRetrieve(payload.tuples, session, payloadFilt)
        return payload.makePayloadEnvelope()

    def _update(self, session, tuples, payloadFilt) -> PayloadEnvelope:
        self._ext.beforeUpdate(tuples, session, payloadFilt)

        # Add everything first.
        for tupleObj in tuples:
            if tupleObj is None:
                raise Exception("None/null was present in array of tuples")

            # Make sure it's not ''
            if tupleObj.id == "":
                tupleObj.id = None

            if tupleObj.id is None:
                session.add(tupleObj)

        self._ext.middleUpdate(tuples, session, payloadFilt)

        # Now merge with the session
        returnTuples = []
        for tupleObj in tuples:
            if tupleObj.id is None:
                # If this was a create, then we can just return this tuple
                returnTuples.append(tupleObj)

            else:
                # Otherwise use the merge method to perform the update magic.
                # and add the resulting merged tuple to the return list
                returnTuples.append(session.merge(tupleObj))

        self._ext.afterUpdate(returnTuples, session, payloadFilt)

        session.commit()

        self._ext.afterUpdateCommit(returnTuples, session, payloadFilt)

        return Payload(tuples=returnTuples).makePayloadEnvelope(result=True)

    def _delete(self, session, tuples, filtId, payloadFilt) -> PayloadEnvelope:
        self._ext.beforeDelete(tuples, session, payloadFilt)

        if len(tuples):
            phIds = [t.id for t in tuples]
        else:
            phIds = [filtId]

        for phId in phIds:
            ph = self._getDeclarativeById(session, phId)
            try:
                # Try to iterate it
                for item in iter(ph):
                    session.delete(item)

            except TypeError:
                # If it's not an iterator
                if ph is not None:
                    session.delete(ph)

        session.commit()

        returnTuples: List[Tuple] = []
        if self._retreiveAll:
            returnTuples = self.createDeclarative(session, payloadFilt)

        self._ext.afterDeleteCommit(tuples, session, payloadFilt)
        return Payload(tuples=returnTuples).makePayloadEnvelope(result=True)

    def sendModelUpdate(
        self, objId, vortexUuid=None, session=None, obj=None, **kwargs
    ):
        session = session if session else self._getSession()
        pl = self._retrieve(session, objId, self._payloadFilter, obj=obj)
        pl.filt.update(self._payloadFilter)
        pl.filt[plIdKey] = objId
        VortexFactory.sendVortexMsg(pl.toVortexMsg(), destVortexUuid=vortexUuid)

    def preProcess(self, payload, vortextUuid, **kwargs):
        pass

    def postProcess(self, action, payloadFilt, vortextUuid):
        pass


class OrmCrudHandlerInThread(OrmCrudHandler):
    def _process(self, payload, **kwargs):
        return deferToThread(OrmCrudHandler.process, self, payload, **kwargs)

    def sendModelUpdate(self, objId, **kwargs):
        return deferToThread(
            OrmCrudHandler.sendModelUpdate, self, objId, **kwargs
        )

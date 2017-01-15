"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

from copy import copy
from typing import Callable
from typing import Union

from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.session import Session
from twisted.internet.threads import deferToThread

from vortex.Payload import Payload, VortexMsgList
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadFilterKeys import plIdKey, plDeleteKey
from vortex.VortexFactory import VortexFactory


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


class _OrmCrudExtensionProcessor(object):
    def __init__(self):
        self.extensions = {}

    def addExtension(self, Tuple):
        def f(cls):
            self.extensions[Tuple.tupleType()] = cls()
            return cls

        return f

    def uiData(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            if not hasattr(tuple_, 'uiData'):
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
                extension.afterUpdateCommit(tuple_, tuples, session, payloadFilt)

        self.uiData(tuples, session, payloadFilt)

    def beforeDelete(self, tuples, session, payloadFilt):
        if not self.extensions:
            return

        for tuple_ in tuples:
            extension = self.extensions.get(tuple_.tupleType())
            if extension:
                extension.beforeDelete(tuple_, tuples, session, payloadFilt)


class OrmCrudHandler(object):
    UPDATE = 1
    CREATE = 2
    DELETE = 3
    QUERY = 4

    def __init__(self, sessionFunctor, Declarative, payloadFilter, retreiveAll=False):
        ''' Create CRUD Hanlder

        This handler will perform crud operations for an SQLAlchemy Declarative

        '''
        self._sessionFunctor = sessionFunctor
        self._Declarative = Declarative
        self._payloadFilter = (payloadFilter
                               if isinstance(payloadFilter, dict) else
                               {"key": payloadFilter})

        self._ep = PayloadEndpoint(self._payloadFilter, self.process)

        self._ext = _OrmCrudExtensionProcessor()

        self._retreiveAll = retreiveAll

    def shutdown(self):
        self._ep.shutdown()

    def addExtension(self, Tuple):
        return self._ext.addExtension(Tuple)

    def process(self, payload,
                vortexUuid: str,
                sendResponse: Callable[[Union[VortexMsgList, bytes]], None],
                              **kwargs):

        # Execute preprocess functions
        if self.preProcess(payload, vortexUuid, **kwargs) != None:
            return

        # Create reply payload replyFilt

        replyFilt = None
        if payload.replyFilt:
            replyFilt = payload.replyFilt
        else:
            replyFilt = copy(self._payloadFilter)
            if payload.filt:
                replyFilt.update(payload.filt)

        # Get data from the payload
        phId = payload.filt.get(plIdKey)
        delete = payload.filt.get(plDeleteKey, False)

        # Setup variables to populate
        replyPayload = None
        action = None

        session = self._getSession()

        # Execute the action
        try:
            if delete == True:
                action = self.DELETE
                replyPayload = self._delete(session, payload.tuples, phId, payload.filt)

            elif len(payload.tuples):
                action = self.UPDATE
                replyPayload = self._update(session, payload.tuples, payload.filt)

            elif phId != None:
                action = self.QUERY
                replyPayload = self._retrieve(session, phId, payload.filt)

            elif len(payload.tuples) == 0:
                action = self.CREATE
                replyPayload = self._create(session, payload.filt)

            else:
                session.close()
                raise Exception("Invalid ORM CRUD parameter state")

        except Exception as e:
            replyPayload = Payload(result=str(e), filt=replyFilt)
            sendResponse(replyPayload.toVortexMsg())
            try:
                session.rollback()
            except:
                pass
            session.close()
            raise

        # Prefer reply filt, if not combine our accpt filt with the filt we were sent

        # Ensure any delegates are playing nice with the result
        if action in (self.DELETE, self.UPDATE) and replyPayload.result == None:
            replyPayload.result = True

        replyPayload.filt = replyFilt
        sendResponse(replyPayload.toVortexMsg())

        # Execute the post process function
        self.postProcess(action, payload.filt, vortexUuid)
        session.commit()
        session.close()

    def _getSession(self):
        if isinstance(self._sessionFunctor, Session):
            return self._sessionFunctor

        return self._sessionFunctor()

    def _getDeclarativeById(self, session, id_):
        qry = session.query(self._Declarative)
        if self._retreiveAll and id_ is not None:
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

    def _create(self, session, payloadFilt):
        tuples = self.createDeclarative(session, payloadFilt)
        payload = Payload(tuples=tuples)
        self._ext.afterCreate(payload.tuples, session, payloadFilt)
        return payload

    def _retrieve(self, session, filtId, payloadFilt, obj=None, **kwargs):
        ph = obj if obj else self._getDeclarativeById(session, filtId)
        payload = Payload()
        payload.tuples = [ph] if ph else []
        self._ext.afterRetrieve(payload.tuples, session, payloadFilt)
        return payload

    def _update(self, session, tuples, payloadFilt):
        self._ext.beforeUpdate(tuples, session, payloadFilt)

        # Add everything first.
        for tuple_ in tuples:
            if tuple == None:
                continue

            # Make sure it's not ''
            if tuple_.id == '':
                tuple_.id = None

            if tuple_.id == None:
                session.add(tuple_)

        self._ext.middleUpdate(tuples, session, payloadFilt)

        # Now merge with the session
        returnTuples = []
        for tuple_ in tuples:
            if tuple == None:
                returnTuples.append(None)
                continue

            # Merge into the session
            returnTuples.append(session.merge(tuple_))

        self._ext.afterUpdate(returnTuples, session, payloadFilt)

        session.commit()

        self._ext.afterUpdateCommit(returnTuples, session, payloadFilt)

        return Payload(tuples=returnTuples, result=True)

    def _delete(self, session, tuples, filtId, payloadFilt):
        self._ext.beforeDelete(tuples, session, payloadFilt)
        phId = tuples[0].id if len(tuples) else filtId

        ph = self._getDeclarativeById(session, phId)
        if ph is not None:
            session.delete(ph)
            session.commit()
        return Payload(result=True)

    def sendModelUpdate(self, objId,
                        vortexUuid=None,
                        session=None,
                        obj=None,
                        **kwargs):
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
        return deferToThread(OrmCrudHandler.sendModelUpdate, self, objId, **kwargs)

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

from vortex.PayloadEndpoint import PayloadEndpoint
from twisted.internet.defer import fail, Deferred
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure

from vortex.Payload import Payload
from vortex.Vortex import vortexSendPayload, vortexSendVortexMsg

logger = logging.getLogger(__name__)


class AsyncModelHandler(object):
    def __init__(self, payloadFilter):
        ''' Create Model Hanlder

        This handler will perform send a list of tuples built by buildModel

        '''
        self._payloadFilter = (payloadFilter
                               if isinstance(payloadFilter, dict) else
                               {"key": payloadFilter})

        self._ep = PayloadEndpoint(self._payloadFilter, self._process)

    def _process(self, payload, vortexUuid, **kwargs):
        AsyncModelHandler.sendModelUpdate(self,
                                          payload=payload,
                                          vortexUuid=vortexUuid,
                                          **kwargs)

    def sendModelUpdate(self,
                        payload=None,
                        vortexUuid=None,
                        vtSessionUuid=None,
                        vtSession=None,
                        responseFilt=None,
                        **kwargs):

        # Prefer reply filt, if not combine our accpt filt with the filt we were sent
        if responseFilt == None:
            responseFilt = copy(self._payloadFilter)
            if payload.filt:
                responseFilt.update(payload.filt)

        # Handle errbacks
        def errback(failure):
            vortexSendPayload(Payload(filt=responseFilt,
                                      result=str(failure.value)),
                              vortexUuid)

            logger.error("AsyncModelHandler, filt=%s\n%s"
                         % (self._payloadFilter, str(failure.value)))

            return failure

        def callback(value):
            # Add some convenience handlers.
            if isinstance(value, list):
                value = Payload(filt=responseFilt, tuples=value)

            if isinstance(value, Payload):
                vortexSendPayload(payload, vortexUuid=vortexUuid)

            if isinstance(value, str):
                vortexSendVortexMsg(value, vortexUuid=vortexUuid)

            return True

        try:
            d = self.buildModel(receivedPayload=payload,
                                vortexUuid=vortexUuid,
                                vtSessionUuid=vtSessionUuid,
                                vtSession=vtSession,
                                responseFilt=responseFilt,
                                **kwargs)

            assert isinstance(d, Deferred)
            d.addCallback(callback)
            d.addErrback(errback)

            return d

        except Exception as e:
            errback(Failure(e))
            raise

    def buildModel(self, **kwargs):
        return deferToThread(self.buildModelBlocking, **kwargs)

    def buildModelBlocking(self, **kwargs):
        return fail(Failure(NotImplementedError(self.__class__.__name__)))


class SendAllModelHandler(AsyncModelHandler):
    def __init__(self, payloadFilter, OrmClass, ormSessionFunction):
        AsyncModelHandler.__init__(self, payloadFilter)
        self._OrmClass = OrmClass
        self._ormSessionFunction = ormSessionFunction

    def buildModelBlocking(self, receivedPayload=None,
                           vortexUuid=None, responseFilt=None, **kwargs):
        ormSession = self._ormSessionFunction()
        tuples = ormSession.query(self._OrmClass).all()

        payload = Payload(filt=responseFilt, tuples=tuples)

        vortexSendVortexMsg(payload.toVortexMsg(), vortexUuid=vortexUuid)

        ormSession.close()

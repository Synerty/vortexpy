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

from twisted.internet.defer import Deferred, fail, succeed
from twisted.python import failure

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)


class ModelHandler(object):
    def __init__(self, payloadFilter):
        ''' Create Model Hanlder

        This handler will perform send a list of tuples built by buildModel

        '''
        self._payloadFilter = (payloadFilter
                               if isinstance(payloadFilter, dict) else
                               {"key": payloadFilter})

        self._ep = PayloadEndpoint(self._payloadFilter, self._process)

    def _process(self, payload: Payload, vortexUuid: str, **kwargs):
        # Execute preprocess functions
        self.preProcess(payload, vortexUuid, **kwargs)

        ModelHandler.sendModelUpdate(self,
                                     vortexUuid=vortexUuid,
                                     payload=payload,
                                     payloadReplyFilt=payload.replyFilt,
                                     **kwargs)

        # Execute the post process function
        self.postProcess(payload.filt, vortexUuid)

    def sendModelUpdate(self, vortexUuid=None,
                        payload=None,
                        payloadReplyFilt=None,
                        userAccess=None,
                        **kwargs):

        payloadFilt = payload.filt if payload else None

        # Prefer reply filt, if not combine our accpt filt with the filt we were sent
        filt = None
        if payloadReplyFilt:
            filt = payloadReplyFilt
        else:
            filt = copy(self._payloadFilter)
            if payloadFilt:
                filt.update(payloadFilt)

        def _sendModelUpdateCallback(value):
            # Add some convenience handlers.
            if isinstance(value, list):
                value = Payload(filt=filt, tuples=value)

            if isinstance(value, Payload):
                VortexFactory.sendVortexMsg(payload.toVortexMsg(),
                                            destVortexUuid=vortexUuid)

            if isinstance(value, bytes):
                VortexFactory.sendVortexMsg(value, destVortexUuid=vortexUuid)

            return True

        def _sendModelUpdateErrback(failure):

            encodedXml = Payload(filt=filt, result=str(failure.value)).toVortexMsg()
            VortexFactory.sendVortexMsg(encodedXml, destVortexUuid=vortexUuid)

            logger.error("Payload filt is : %s", payload.filt)
            logger.exception(failure.value)

            return failure

        result = self.buildModel(payloadFilt=payload.filt if payload else None,
                                 payload=payload,
                                 vortexUuid=vortexUuid,
                                 userAccess=userAccess,
                                 **kwargs)

        if isinstance(result, Deferred):
            d = result
        elif isinstance(result, failure.Failure):
            d = fail(result)
        else:
            d = succeed(result)

        d.addCallback(_sendModelUpdateCallback)
        d.addErrback(_sendModelUpdateErrback)

        # deferToThread doesn't like this, and it never used to return anything anyway
        # return d

    def shutdown(self):
        self._ep.shutdown()

    def buildModel(self, payloadFilt=None,
                   vortexUuid=None,
                   userAccess=None,
                   payload=Payload(),
                   session=None):
        raise NotImplementedError()

    def preProcess(self, payload, vortextUuid, **kwargs):
        pass

    def postProcess(self, payloadFilt, vortextUuid):
        pass

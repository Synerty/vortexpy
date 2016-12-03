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

from twisted.internet.defer import maybeDeferred

from vortex.Payload import Payload
from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.Vortex import vortexSendVortexMsg

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

    def _process(self, payload, vortexUuid, session, **kwargs):
        # Execute preprocess functions
        self.preProcess(payload, vortexUuid, **kwargs)

        ModelHandler.sendModelUpdate(self,
                                     vortexUuid=vortexUuid,
                                     payload=payload,
                                     payloadReplyFilt=payload.replyFilt,
                                     session=session)

        # Execute the post process function
        self.postProcess(payload.filt, vortexUuid)

    def sendModelUpdate(self, vortexUuid=None,
                        payload=None, payloadReplyFilt=None,
                        session=None, userAccess=None):

        payloadFilt = payload.filt if payload else None

        # Prefer reply filt, if not combine our accpt filt with the filt we were sent
        filt = None
        if payloadReplyFilt:
            filt = payloadReplyFilt
        else:
            filt = copy(self._payloadFilter)
            if payloadFilt:
                filt.update(payloadFilt)

        def _sendModelUpdateCallback(tuples):

            assert tuples is not None

            encodedXml = Payload(filt=filt, tuples=tuples).toVortexMsg()
            vortexSendVortexMsg(encodedXml, vortexUuid=vortexUuid)

        def _sendModelUpdateErrback(failure):

            encodedXml = Payload(filt=filt, result=str(failure.value)).toVortexMsg()
            vortexSendVortexMsg(encodedXml, vortexUuid=vortexUuid)

            logger.error("Payload filt is : %s", payload.filt)
            logger.exception(failure.value)

            return failure

        d = maybeDeferred(self.buildModel,
                          payloadFilt=payload.filt if payload else None,
                          payload=payload,
                          vortexUuid=vortexUuid,
                          userAccess=userAccess)
        d.addCallback(_sendModelUpdateCallback)
        d.addErrback(_sendModelUpdateErrback)
        # deferToThread doesn't like this, and it never used to return anything anyway
        # return d

    def buildModel(self, payloadFilt=None,
                   vortexUuid=None,
                   userAccess=None,
                   payload=Payload()):
        raise NotImplementedError()

    def preProcess(self, payload, vortextUuid, **kwargs):
        pass

    def postProcess(self, payloadFilt, vortextUuid):
        pass

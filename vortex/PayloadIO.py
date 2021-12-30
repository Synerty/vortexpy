"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from datetime import datetime

import pytz
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from vortex.DeferUtil import vortexLogFailure
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.VortexABC import SendVortexMsgResponseCallable

logger = logging.getLogger(name="PayloadIO")


class PayloadIO(object):
    """
    PayloadIO, Processes payloads received from the vortex and distributes
    them to where they need to go.
    """

    # Singleton
    _instance = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(PayloadIO, cls).__new__(cls)
            cls._instance.__singleton_init__()
        return cls._instance

    def __singleton_init__(self):
        self._endpoints = set()

    def remove(self, endpoint):
        if endpoint in self._endpoints:
            self._endpoints.remove(endpoint)

    def add(self, endpoint):
        self._endpoints.add(endpoint)

    @property
    def endpoints(self):
        """Endpoints property

        @:return a copy of the list of endpoints
        """
        return list(self._endpoints)

    def process(
        self,
        payloadEnvelope: PayloadEnvelope,
        vortexUuid: str,
        vortexName: str,
        httpSession,
        sendResponse: SendVortexMsgResponseCallable,
    ):

        immutableEndpoints = list(self._endpoints)
        for endpoint in immutableEndpoints:
            if not endpoint.check(payloadEnvelope, vortexName):
                continue

            reactor.callLater(
                0,
                self._processLater,
                endpoint,
                payloadEnvelope,
                vortexUuid,
                vortexName,
                httpSession,
                sendResponse,
            )

    def _processLater(
        self,
        endpoint,
        payloadEnvelope: PayloadEnvelope,
        vortexUuid: str,
        vortexName: str,
        httpSession,
        sendResponse: SendVortexMsgResponseCallable,
    ):
        startDate = datetime.now(pytz.utc)

        def respondToException(failure):
            """Respond To Exception
            Putting the exception into a failure messes with the stack, hence the
            common function
            """
            try:
                sendResponse(
                    PayloadEnvelope(
                        filt=payloadEnvelope.filt,
                        result=str(failure.getTraceback()),
                    ).toVortexMsg()
                )
            except Exception as e:
                logger.exception(e)

            vortexLogFailure(failure, logger)
            logger.error(payloadEnvelope.filt)

        def callback(value, blocking=False):
            # A blocking call taking more than 100ms is BAD
            # Otherwise a call taking more than a 1s is just poor performance.
            secondsTaken = (datetime.now(pytz.utc) - startDate).total_seconds()
            if secondsTaken > 0.1 and blocking or secondsTaken > 2.0:
                func = logger.critical if blocking else logger.debug
                func(
                    "%s Payload endpoint took %s\npayload.filt=%s\n%s",
                    "BLOCKING " if blocking else "",
                    secondsTaken,
                    payloadEnvelope.filt,
                    endpoint,
                )

        try:
            d = endpoint.process(
                payloadEnvelope,
                vortexUuid,
                vortexName,
                httpSession,
                sendResponse,
            )
            if isinstance(d, Deferred):
                d.addCallback(callback)
                d.addErrback(respondToException)
            elif hasattr(d, "addCallback"):
                raise Exception("isinstance FAILED")
            else:
                callback(True, blocking=True)

        except Exception as e:
            respondToException(Failure(e))

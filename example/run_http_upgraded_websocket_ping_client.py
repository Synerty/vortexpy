import logging
from datetime import datetime

import pytz
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks

from example.setup_example_logging import setupExampleLogging
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadResponse import PayloadResponse
from vortex.VortexClientWebsocketFactory import VortexClientWebsocketFactory

logger = logging.getLogger(__name__)


setupExampleLogging()

if __name__ == "__main__":
    import sys

    from twisted.python import log
    from twisted.internet import reactor, ssl

    log.startLogging(sys.stdout)

    @inlineCallbacks
    def sendMessage():
        factory = VortexClientWebsocketFactory(
            "test_websocket", url="ws://127.0.0.1:9000/vortexws"
        )
        yield factory.connect("127.0.0.1", 9000, ssl.ClientContextFactory())

        filt = {
            "key": "ws_test",
        }

        while True:
            payloadEnvelope = PayloadEnvelope(filt)
            payloadEnvelope.encodedPayload = b"*" * 163880
            responseDeferred = PayloadResponse(payloadEnvelope)
            logger.info(payloadEnvelope.date)

            # factory.sendVortexMsg(payload.toVortexMsg())

            factory.sendVortexMsg(
                payloadEnvelope.toVortexMsg(base64Encode=False)
            )
            logger.info(payloadEnvelope.date)

            yield responseDeferred
            logger.info(
                "CALL COMPLETE, Time Taken: %s",
                datetime.now(pytz.utc) - payloadEnvelope.date,
            )

            yield task.deferLater(reactor, 5, lambda: None)

    reactor.callLater(0.5, sendMessage)

    reactor.run()

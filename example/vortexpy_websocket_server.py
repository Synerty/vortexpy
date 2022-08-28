import logging
import os
from datetime import datetime

from twisted.web import server
from txhttputil.site.FileUnderlayResource import FileUnderlayResource
from txwebsocket.txws import WebSocketUpgradeHTTPChannel

from vortex.PayloadEndpoint import PayloadEndpoint
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.VortexABC import SendVortexMsgResponseCallable
from vortex.VortexFactory import VortexFactory

logger = logging.getLogger(__name__)

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
#                     datefmt="%Y-%m-%d %H:%M:%S")

logger.setLevel(logging.DEBUG)

console = logging.StreamHandler()
logger.addHandler(console)


class MyFormatter(logging.Formatter):
    converter = datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (t, record.msecs)
        return s


formatter = MyFormatter(fmt='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d,%H:%M:%S.%f')
console.setFormatter(formatter)

if __name__ == "__main__":
    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    resource = FileUnderlayResource()
    resource.enableSinglePageApplication()
    resource.addFileSystemRoot(
        os.path.dirname(os.path.realpath(__file__)) + "/test_serve"
    )

    site = server.Site(resource)
    site.protocol = WebSocketUpgradeHTTPChannel


    class Test:
        def logDate(
                self,
                payloadEnvelope: PayloadEnvelope,
                sendResponse: SendVortexMsgResponseCallable,
                *args,
                **kwargs
        ):
            logger.info(payloadEnvelope.filt)
            logger.info(payloadEnvelope.date)

            sendResponse(payloadEnvelope.toVortexMsg())


    test = Test()
    endpoint = PayloadEndpoint(
        filt={
            "key": "ws_test",
        },
        callable_=test.logDate,
    )

    # VortexFactory.createWebsocketServer("test_websocket", 9000)

    VortexFactory.createHttpWebsocketServer("vortexws", resource)
    reactor.listenTCP(9000, site)

    reactor.run()

    endpoint.shutdown()

import logging
import os

from twisted.internet import reactor
from twisted.web import server
from txwebsocket.txws import WebSocketUpgradeHTTPChannel

from example.setup_example_logging import setupExampleLogging
from txhttputil.site.FileUnderlayResource import FileUnderlayResource
from vortex.VortexFactory import VortexFactory
from vortex.rpc.test.RPCTest import myRemoteAddMethod
from vortex.rpc.test.RPCTest import myRemoteExceptionMethod

logger = logging.getLogger(__name__)

setupExampleLogging()

if __name__ == "__main__":
    # BEGIN WEBSOCKET HTTP UPGRADED CLIENT CONNECT
    resource = FileUnderlayResource()
    resource.enableSinglePageApplication()
    resource.addFileSystemRoot(
        os.path.dirname(os.path.realpath(__file__)) + "/test_serve"
    )

    site = server.Site(resource)
    site.protocol = WebSocketUpgradeHTTPChannel

    VortexFactory.createHttpWebsocketServer("listenVortexName", resource)
    reactor.listenTCP(10101, site)
    # END WEBSOCKET HTTP UPGRADED CLIENT CONNECT

    reactor.callLater(0, logger.info, "RPCTest server running")

    reactor.callLater(0, myRemoteExceptionMethod.start)
    reactor.callLater(0, myRemoteAddMethod.start)
    reactor.run()

import logging

from twisted.internet import reactor

from vortex.VortexFactory import VortexFactory
from vortex.rpc.test.RPCTest import myRemoteAddMethod
from vortex.rpc.test.RPCTest import myRemoteExceptionMethod

logger = logging.getLogger(__name__)
logging.basicConfig()

if __name__ == "__main__":
    # BEGIN WEBSOCKET CLIENT CONNECT
    VortexFactory.createWebsocketServer("listenVortexName", 10102)
    # END WEBSOCKET CLIENT CONNECT

    reactor.callLater(0, logger.info, "RPCTest server running")

    reactor.callLater(0, myRemoteExceptionMethod.start)
    reactor.callLater(0, myRemoteAddMethod.start)
    reactor.run()

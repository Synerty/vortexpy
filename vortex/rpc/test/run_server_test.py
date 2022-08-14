import logging

from twisted.internet import reactor

from vortex.VortexFactory import VortexFactory
from vortex.rpc.test.RPCTest import myRemoteAddMethod

logger = logging.getLogger(__name__)
logging.basicConfig()

if __name__ == "__main__":
    VortexFactory.createWebsocketServer("listenVortexName", 10101)

    reactor.callLater(0, logger.info, "RPCTest server running")
    reactor.callLater(0, myRemoteAddMethod.start)
    reactor.run()

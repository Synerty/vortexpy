import logging

from twisted.internet import reactor
from txhttputil.util.LoggingUtil import setupLogging

from vortex.VortexFactory import VortexFactory
from vortex.rpc.RPC import vortexRPC
from vortex.rpc.RPCTest import myRemoteAddMethod

logger = logging.getLogger(__name__)
setupLogging()



if __name__ == '__main__':
    VortexFactory.createTcpServer("listenVortexName", 10101)
    reactor.callLater(0, logger.info, "RPCTest server running")
    reactor.callLater(0, myRemoteAddMethod.start)
    reactor.run()

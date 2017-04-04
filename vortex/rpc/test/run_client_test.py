import logging

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from txhttputil.util.LoggingUtil import setupLogging

from vortex.DeferUtil import vortexLogFailure
from vortex.VortexFactory import VortexFactory
from vortex.rpc.run_server_test import myRemoteAddMethod

logger = logging.getLogger(__name__)
setupLogging()

@inlineCallbacks
def connect():
    yield VortexFactory.createTcpClient("sendVortexName", "127.0.0.1", 10101)

    reactor.callLater(2, callRpc)

def callRpc():

    d = myRemoteAddMethod(7, kwarg1=4)
    d.addCallback(lambda v: logger.debug("SUCCESSS, result = %s", v))
    d.addErrback(vortexLogFailure, logger)
    d.addBoth(lambda _ : reactor.stop())
    return d


if __name__ == '__main__':

    reactor.callLater(0, logger.info, "RPCTest client running")
    reactor.callLater(0, connect)
    reactor.run()

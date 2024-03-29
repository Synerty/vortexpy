import logging
from datetime import datetime

import pytz
from twisted.internet import reactor
from twisted.internet import task
from twisted.internet.defer import inlineCallbacks

from example.setup_example_logging import setupExampleLogging
from vortex.DeferUtil import vortexLogFailure
from vortex.VortexFactory import VortexFactory
from vortex.rpc.test.RPCTest import myRemoteAddMethod
from vortex.rpc.test.RPCTest import myRemoteExceptionMethod

logger = logging.getLogger(__name__)

setupExampleLogging()


@inlineCallbacks
def connect():
    # BEGIN WEBSOCKET CLIENT CONNECT
    yield VortexFactory.createWebsocketClient(
        "sendVortexName", "127.0.0.1", 10102, "ws://127.0.0.1:10101/vortexws"
    )
    # END WEBSOCKET CLIENT CONNECT

    reactor.callLater(2, callRpcException)
    reactor.callLater(5, callRpc)


def callRpcException():
    d = myRemoteExceptionMethod(7, kwarg1=4)
    d.addCallback(lambda v: logger.debug("SUCCESSS, result = %s", v))
    d.addErrback(vortexLogFailure, logger)
    return d


@inlineCallbacks
def callRpc():
    while True:
        startDate = datetime.now(pytz.utc)
        v = yield myRemoteAddMethod(7, kwarg1=4)
        logger.debug(
            "CALL COMPLETE, result = %s, Time Taken: %s",
            v,
            datetime.now(pytz.utc) - startDate,
        )

        yield task.deferLater(reactor, 5, lambda: None)


if __name__ == "__main__":
    reactor.callLater(0, logger.info, "RPCTest client running")
    reactor.callLater(0, connect)
    reactor.run()

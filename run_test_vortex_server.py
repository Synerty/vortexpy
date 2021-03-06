"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging

from twisted.internet import reactor, defer
from twisted.web import server
from twisted.web.http import HTTPChannel
from twisted.web.resource import Resource

from vortex.VortexFactory import VortexFactory
from vortex.test.TupleDataObservableTestHandler import NotifyTestTimer

logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s:%(message)s'
                    , datefmt='%d-%b-%Y %H:%M:%S'
                    , level=logging.DEBUG)

logger = logging.getLogger(__name__)


class TestRootResource(Resource):
    def getChildWithDefault(self, path, request):
        return Resource.getChildWithDefault(self, path, request)

    def getChild(self, path, request):
        return Resource.getChild(self, path, request)


def setupVortexServer(portNum=8345, wsPortNum=8344):
    """ Setup Site
    Sets up the web site to listen for connections and serve the site.
    Supports customisation of resources based on user details

    @return: Port object
    """
    defer.setDebugging(True)

    # Register the test tuple
    from vortex.test import TestTuple
    TestTuple.__unused = False  # Crap code

    from vortex.test import VortexJSTupleLoaderTestHandler
    VortexJSTupleLoaderTestHandler.__unused = False  # Crap code

    rootResource = TestRootResource()
    VortexFactory.createServer("default", rootResource)
    VortexFactory.createWebsocketServer("default", wsPortNum)
    # rootResource.putChild(b"vortex", VortexResource())

    site = server.Site(rootResource)
    site.protocol = HTTPChannel

    port = reactor.listenTCP(portNum, site).port

    # ip = subprocess.getoutput("/sbin/ifconfig").split("\n")[1].split()[1][5:]

    logger.info('VortexServer test is alive and listening on port %s', port)
    logger.info('VortexServerWebsocket test is alive and listening on port %s', wsPortNum)
    logger.debug("Logging debug messages enabled")

    NotifyTestTimer.startTupleUpdateNotifyer()

    return port


if __name__ == '__main__':
    setupVortexServer()
    reactor.run()

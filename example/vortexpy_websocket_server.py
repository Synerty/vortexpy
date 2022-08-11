from twisted.web import server
from txhttputil.site.FileUnderlayResource import FileUnderlayResource
import os

from vortex.VortexFactory import VortexFactory

if __name__ == '__main__':
    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    resource = FileUnderlayResource()
    resource.enableSinglePageApplication()
    resource.addFileSystemRoot(
        os.path.dirname(os.path.realpath(__file__)) + "/test_serve")

    site = server.Site(resource)

    # VortexFactory.createWebsocketServer("test_websocket", 9000)
    VortexFactory.createHttpWebsocketServer("test_websocket", resource)

    reactor.listenTCP(9000, site)

    reactor.run()

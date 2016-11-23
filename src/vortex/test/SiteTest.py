"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging

from twisted.internet import reactor
from twisted.web import server
from twisted.web.http import HTTPChannel
from twisted.web.resource import Resource

from vortex.VortexResource import VortexResource

logger = logging.getLogger(__name__)


def setupVortexServer(portNum=8345):
    ''' Setup Site
    Sets up the web site to listen for connections and serve the site.
    Supports customisation of resources based on user details

    @return: Port object
    '''

    rootResource = Resource()
    rootResource.putChild("/vortex", VortexResource)

    site = server.Site(rootResource)
    site.protocol = HTTPChannel

    port = reactor.listenTCP(portNum, site).port

    import subprocess
    ip = subprocess.getoutput("/sbin/ifconfig").split("\n")[1].split()[1][5:]

    logger.info('Vortex test is alive and listening on http://%s:%s/vortex',
                 ip, port)
    return port

if __name__ == '__main__':
    setupVortexServer()
    reactor.run()
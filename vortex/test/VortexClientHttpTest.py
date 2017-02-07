import unittest

from twisted.internet import reactor
from twisted.internet.defer import DeferredList
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet.protocol import Factory, Protocol, connectionDone

from vortex.Payload import Payload
from vortex.VortexClient import VortexClient

class DummyListenerProtocol(Protocol):

    def connectionMade(self):
        pass

    def dataReceived(self, data):
        pass

    def connectionLost(self, reason=connectionDone):
        pass


class DummyListenerFactory(Factory):

    def buildProtocol(self, addr):
        return DummyListenerProtocol()



class VortexClientTest(unittest.TestCase):
    def setUp(self):
        port = 9009
        host = "localhost"

        def listenSuccess(listeningPort):
            self.listeningPort = listeningPort

            self.vortexClient = VortexClient()
            return self.vortexClient.connect(host, port)

        endpoint = TCP4ServerEndpoint(reactor, port)
        d = endpoint.listen(DummyListenerFactory())
        d.addCallback(listenSuccess)

        return d

    def tearDown(self):
        try:
            self.vortexClient.disconnect()
        except:
            pass

        self.listeningPort.stopListening()

    def testSend(self):
        deferreds = []

        # Just test functionality.
        d = self.vortexClient.send(Payload())
        deferreds.append(d)

        d = self.vortexClient.send([Payload(), Payload()])
        deferreds.append(d)

        d = self.vortexClient.sendVortexMsg(Payload().toVortexMsg())
        deferreds.append(d)

        d = self.vortexClient.sendVortexMsg([Payload().toVortexMsg(), Payload().toVortexMsg()])
        deferreds.append(d)

        return DeferredList(deferreds)


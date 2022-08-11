from vortex.VortexClientWebsocketFactory import VortexClientWebsocketFactory
from vortex.PayloadEnvelope import PayloadEnvelope

if __name__ == "__main__":
    import sys

    from twisted.python import log
    from twisted.internet import reactor

    log.startLogging(sys.stdout)

    factory = VortexClientWebsocketFactory("test_websocket")
    factory.connect("localhost", 9000)

    def sendMessage():
        filt = {"test": "Hello, World!"}
        factory.sendVortexMsg(PayloadEnvelope(filt=filt).toVortexMsg())

    reactor.callLater(0.5, lambda: sendMessage)

    reactor.run()

from vortex.PayloadFilterKeys import rapuiServerEcho
from vortex.VortexClientWebsocketFactory import VortexClientWebsocketFactory
from vortex.PayloadEnvelope import PayloadEnvelope

if __name__ == "__main__":
    import sys

    from twisted.python import log
    from twisted.internet import reactor, ssl

    log.startLogging(sys.stdout)

    factory = VortexClientWebsocketFactory(
        "test_websocket", url="ws://127.0.0.1:9000/vortexws"
    )
    factory.connect("127.0.0.1", 9000, ssl.ClientContextFactory())

    def sendMessage():
        filt = {"test": "Hello, World!", rapuiServerEcho: "anything"}
        factory.sendVortexMsg(PayloadEnvelope(filt=filt).toVortexMsg())

    reactor.callLater(0.5, lambda: sendMessage)

    reactor.run()

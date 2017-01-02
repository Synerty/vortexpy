from twisted.internet import defer
from twisted.trial import unittest

from vortex.Payload import Payload
from vortex.PayloadIO import PayloadIO
from vortex.PayloadResponse import PayloadResponse


class PayloadResponseTest(unittest.TestCase):
    def testSuccesCb(self):
        payload = Payload(filt={"key": "PayloadResponseTest"})

        payloadReponse = PayloadResponse(payload)
        payloadReponse.addCallback(lambda pl: self.assertIs(pl, payload))
        payloadReponse.addErrback(lambda pl: self.assertTrue(False))

        PayloadIO().process(payload, "", "", None, lambda _: None)

        return payloadReponse

    def testTimeoutErrback(self):
        payload = Payload(filt={"key": "PayloadResponseTest"})

        payloadReponse = PayloadResponse(payload, timeout=3)
        payloadReponse.addCallback(lambda pl: self.assertFalse(True))
        self.assertFailure(payloadReponse, defer.TimeoutError)

        # PayloadIO().process(payload)

        return payloadReponse

    def testSerialiseDeserialise(self):
        payload = Payload(filt={"key": "PayloadResponseTest"})

        d = PayloadResponse(payload, timeout=0)
        d.addErrback(lambda _: True)  # Handle the errback

        vortexMsg = payload.toVortexMsg()
        Payload().fromVortexMsg(vortexMsg)

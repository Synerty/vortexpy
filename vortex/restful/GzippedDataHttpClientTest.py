import gzip
import logging
from datetime import datetime
from typing import List

from twisted.trial import unittest
from twisted.internet.error import TimeoutError as TwistedTimeoutError

from vortex.Tuple import Tuple, TupleField, addTupleType
from vortex.restful.GzippedDataHttpClient import GzippedDataHttpClient
from vortex.restful.RestfulResource import HTTP_REQUEST

logger = logging.getLogger(__name__)


@addTupleType
class GzippedDataHttpResponseTuple(Tuple):
    __tupleType__ = "GzippedDataHttpResponseTuple"

    requestDate: datetime = TupleField()
    responseDate: datetime = TupleField()
    code: int = TupleField(defaultValue=0)
    version: tuple = TupleField()  # (b'HTTP', 1, 1)
    headers: dict[str, str] = TupleField()
    body: bytes = TupleField()
    exceptions: List[Exception] = TupleField(defaultValue=[])


class GzippedDataHttpClientTest(unittest.TestCase):
    def setUp(self):
        self.payload = gzip.compress(b"test")
        self.headers = {}
        self.meta = GzippedDataHttpResponseTuple()

    def testHttpPost(self):
        client = GzippedDataHttpClient(
            url="http://httpbin.org/post",
            payload=self.payload,
            headers=self.headers,
            method=HTTP_REQUEST.POST,
            meta=GzippedDataHttpResponseTuple(),
            isPayloadGzipped=True,
            compressed=True,
        )
        d = client.run()
        d.addCallback(self._assertValidResponse)
        return d

    def testHttpsPost(self):
        client = GzippedDataHttpClient(
            url="https://httpbin.org/post",
            payload=self.payload,
            headers=self.headers,
            method=HTTP_REQUEST.POST,
            meta=GzippedDataHttpResponseTuple(),
            isPayloadGzipped=True,
            compressed=True,
        )
        d = client.run()
        d.addCallback(self._assertValidResponse)
        return d

    def testPlaintextDataPost(self):
        payload = gzip.decompress(self.payload)
        client = GzippedDataHttpClient(
            url="http://httpbin.org/post",
            payload=payload,
            headers=self.headers,
            method=HTTP_REQUEST.POST,
            meta=GzippedDataHttpResponseTuple(),
            isPayloadGzipped=False,
            compressed=True,
        )
        d = client.run()
        d.addCallback(self._assertValidResponse)
        return d

    def testConnectTimeout(self):
        client = GzippedDataHttpClient(
            url="http://example.com:19839",
            payload=self.payload,
            headers=self.headers,
            method=HTTP_REQUEST.POST,
            meta=GzippedDataHttpResponseTuple(),
            isPayloadGzipped=False,
            compressed=True,
            timeout=1.0,
        )
        d = client.run()
        d.addCallback(self._assertTimeoutException)
        return d

    def _assertValidResponse(self, tuple_: GzippedDataHttpResponseTuple):
        self.assertEqual(tuple_.code, 200)
        self.assertTrue(
            (tuple_.responseDate - tuple_.requestDate).total_seconds() > 0
        )

    def _assertTimeoutException(self, tuple_: GzippedDataHttpResponseTuple):
        booleans = [
            isinstance(e, TwistedTimeoutError) for e in tuple_.exceptions
        ]
        self.assertTrue(any(booleans))

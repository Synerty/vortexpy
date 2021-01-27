import io
import json
import logging

from vortex.restful.RestfulResource import HTTP_REQUEST
from vortex.restful.RestfulResource import PluginRestfulResource
from twisted.trial import unittest
from twisted.web.test.requesthelper import DummyRequest
from vortex.Tuple import Tuple
from vortex.Tuple import TupleField
from vortex.Tuple import addTupleType

logger = logging.getLogger(__name__)


@addTupleType
class DummyTuple(Tuple):
    __tupleType__ = "DummyTuple"

    id = TupleField()
    text = TupleField()


class TestHandlerController:
    def testMethod(self, tuple_: DummyTuple):
        if hasattr(tuple_, "tupleToRestfulJsonDict"):
            # vortexpy > 2.1.3
            return tuple_.tupleToRestfulJsonDict()
        else:
            return tuple_.tupleToSmallJsonDict()


class RestfulServerTest(unittest.TestCase):
    def setUp(self):
        handlerController = TestHandlerController()
        self.pluginAResource = PluginRestfulResource()
        self.pluginAResource.registerMethod(
            handlerController.testMethod,
            DummyTuple,
            b"test",
            [HTTP_REQUEST.GET, HTTP_REQUEST.POST],
        )
        self.pluginAResource.registerMethod(
            handlerController.testMethod,
            DummyTuple,
            b"test/test",
            [HTTP_REQUEST.GET, HTTP_REQUEST.POST],
        )
        self.pluginAResource.registerMethod(
            handlerController.testMethod,
            DummyTuple,
            b"test/test/test",
            [HTTP_REQUEST.GET, HTTP_REQUEST.POST],
        )

    def _dictToBytes(self, dictionary: dict) -> bytes:
        return bytes(json.dumps(dictionary), "utf-8")

    def _bytesToDict(self, bytes_: bytes) -> dict:
        string = bytes_[0].decode("utf-8")
        return json.loads(string)

    def _check(self, expected: dict, actual: dict) -> bool:
        # expected dict should be equal or a subnet of actual dict
        return expected.items() <= actual.items()

    def testValidJsonRequest(self):
        requestDict = {"id": 1, "text": "text"}

        request = DummyRequest([])
        request.content = io.BytesIO(self._dictToBytes(requestDict))

        jsonResource = self.pluginAResource.getChild(b"test", request)

        jsonResource.render(request)
        self.assertEqual(request.responseCode, 200)
        responseDict = self._bytesToDict(request.written)
        self.assertTrue(self._check(requestDict, responseDict))

    def testInvalidJsonRequest(self):
        request = DummyRequest([])
        request.content = io.BytesIO(b"##invalid json}")

        jsonResource = self.pluginAResource.getChild(b"test", request)
        jsonResource.render(request)
        self.assertEqual(request.responseCode, 500)

    def testPath(self):
        requestDict = {"id": 1, "text": "text"}

        for path in [b"test", b"test/test", b"test/test/test"]:
            request = DummyRequest([])
            request.content = io.BytesIO(self._dictToBytes(requestDict))

            jsonResource = self.pluginAResource.getChild(path, request)

            jsonResource.render(request)
            self.assertEqual(request.responseCode, 200)
            responseDict = self._bytesToDict(request.written)
            self.assertTrue(self._check(requestDict, responseDict))

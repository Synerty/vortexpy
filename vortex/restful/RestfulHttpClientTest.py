import json
import logging
from typing import Annotated, List

from twisted.trial import unittest

from vortex.Tuple import (
    Tuple,
    TupleField,
    addTupleType,
    IntTupleFieldValidator,
    TupleFieldValidatorABC,
)
from vortex.restful.RestfulHttpClient import RestfulHttpClient
from vortex.restful.RestfulResource import HTTP_REQUEST

logger = logging.getLogger(__name__)


@addTupleType
class TestTuple(Tuple):
    __tupleType__ = "TestTuple"
    id: int = TupleField()
    count: Annotated[int, IntTupleFieldValidator(0, 100)] = TupleField()
    text: str = TupleField()


@addTupleType
class ResponseTuple(Tuple):
    __tupleType__ = "ResponseTuple"
    data: str = TupleField(comment="request in string")
    json: TestTuple = TupleField(comment="request converted to jsonDict")


class RestfulClientTest(unittest.TestCase):
    def setUp(self):
        self.tuple = TestTuple()
        self.tuple.id = 10
        self.tuple.count = 50
        self.tuple.text = "test"

    def testHttpPost(self):
        client = RestfulHttpClient(
            "http://httpbin.org/anything",
            method=HTTP_REQUEST.POST,
            postTuple=self.tuple,
            ResponseTuple=ResponseTuple,
        )
        d = client.run()
        d.addCallback(self._checkPostJSON)
        # d.addCallback(self.print)
        return d

    def _checkPostJSON(self, tuple_):
        self.assertTrue(isinstance(tuple_, ResponseTuple))
        self.assertEqual(
            tuple_.data, json.dumps(self.tuple.tupleToRestfulJsonDict())
        )
        self.assertEqual(tuple_.json.id, self.tuple.id)
        self.assertEqual(tuple_.json.text, self.tuple.text)

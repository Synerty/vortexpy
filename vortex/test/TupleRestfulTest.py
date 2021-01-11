"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""

import datetime
import json
from typing import Dict
from typing import List
from typing import Union

from twisted.trial import unittest

from vortex.Tuple import TUPLE_TYPES_BY_NAME, TupleHash
from vortex.Tuple import Tuple
from vortex.Tuple import TupleField
from vortex.Tuple import addTupleType


@addTupleType
class RestfulTestTuple(Tuple):
    """
    classdocs
    """

    __tupleType__ = "synerty.vortex.RestfulTestTuple"
    aInt: int = TupleField()
    aFloat: float = TupleField()
    aString: str = TupleField()
    aBoolTrue: bool = TupleField()
    aBoolFalse: bool = TupleField()
    aList: List[Union[str, int, bool, float]] = TupleField()
    aDict: Dict[str, Union[str, float]] = TupleField()
    aSubTuple: "RestfulSubTestTuple" = TupleField()
    aListOfSubTuples: List["RestfulSubTestTuple"] = TupleField()
    aDictOfSubTuples: Dict[str, "RestfulSubTestTuple"] = TupleField()


@addTupleType
class RestfulSubTestTuple(Tuple):
    """Test Sub Tuple
    This tuple will test if we can have tuples in tuples and still serialise and
    deserialise them.
    """

    __tupleType__ = "rapui.synerty.RestfulSubTestTuple"
    subInt: int = TupleField()


def _makeRestfultTestTuple():
    tuple_ = RestfulTestTuple()
    tuple_.aBoolTrue = True
    tuple_.aBoolFalse = False
    tuple_.aFloat = 2.56
    tuple_.aInt = 1231231
    tuple_.aString = "test string from 345345345@$#%#$%#$%#$%#"
    tuple_.aList = ["y", 3, True, 3.3]
    tuple_.aDict = {
        "a": "test str",
        "1": 1.234,
    }  # Key strings are loaded as unicode
    tuple_.aDictOfSubTuples = {}

    subTuple = RestfulSubTestTuple()
    subTuple.subInt = 2048
    tuple_.aSubTuple = subTuple

    tuple_.aListOfSubTuples = []
    for x in range(2):
        subTuple = RestfulSubTestTuple()
        subTuple.subInt = x
        tuple_.aListOfSubTuples.append(subTuple)

        subTuple = RestfulSubTestTuple()
        subTuple.subInt = x + 10
        tuple_.aDictOfSubTuples[str(x)] = subTuple

    return tuple_


class TupleRestfulTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testToRestfulPass(self):
        # Create tuple
        origTuple = _makeRestfultTestTuple()

        # To Restful JSON Object
        jsonDict = origTuple.tupleToRestfulJsonDict()

        # Pass it through the JSON object to JSON string conversion
        jsonDict = json.loads(json.dumps(jsonDict))

        # Reconstruct a new Tuple
        deserialisedTuple = RestfulTestTuple().restfulJsonDictToTupleWithValidation(
            jsonDict, RestfulTestTuple
        )

        self.assertEqual(TupleHash(origTuple), TupleHash(origTuple))
        self.assertEqual(TupleHash(deserialisedTuple), TupleHash(deserialisedTuple))

        self.assertEqual(
            TupleHash(origTuple),
            TupleHash(deserialisedTuple),
            "Tuple serialise -> deserialise error",
        )

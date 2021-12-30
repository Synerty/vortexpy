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
from typing import Dict, Annotated
from typing import List
from typing import Union

from ddt import data, ddt
from twisted.trial import unittest

from vortex.Tuple import TUPLE_TYPES_BY_NAME, TupleHash, IntTupleFieldValidator
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
    aIntWithValidator: Annotated[
        int, IntTupleFieldValidator(1, 100)
    ] = TupleField()
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
    tuple_.aIntWithValidator = 50
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


@ddt
class TupleRestfulTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _testSerialisation(self, origTuple):
        # To Restful JSON Object
        jsonDict = origTuple.tupleToRestfulJsonDict()
        # Pass it through the JSON object to JSON string conversion
        jsonDict = json.loads(json.dumps(jsonDict))
        # Reconstruct a new Tuple
        deserialisedTuple = (
            RestfulTestTuple().restfulJsonDictToTupleWithValidation(
                jsonDict, RestfulTestTuple
            )
        )
        return deserialisedTuple

    @data(-1, 110)
    def testIntValidatorInvalid(self, int_):
        # Create tuple
        origTuple = _makeRestfultTestTuple()
        origTuple.aIntWithValidator = int_
        self.assertRaises(ValueError, self._testSerialisation, origTuple)

    @data(1, 99)
    def testIntValidatorValid(self, int_):
        # Create tuple
        origTuple = _makeRestfultTestTuple()
        origTuple.aIntWithValidator = int_
        self.assertEqual(
            self._testSerialisation(origTuple).aIntWithValidator, int_
        )

    def testValidatorPass(self):
        # Create tuple
        origTuple = _makeRestfultTestTuple()

        deserialisedTuple = self._testSerialisation(origTuple)

        self.assertEqual(TupleHash(origTuple), TupleHash(origTuple))
        self.assertEqual(
            TupleHash(deserialisedTuple), TupleHash(deserialisedTuple)
        )
        self.assertEqual(
            TupleHash(origTuple),
            TupleHash(deserialisedTuple),
            "Tuple serialise -> deserialise error",
        )

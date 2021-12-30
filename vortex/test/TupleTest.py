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

from twisted.trial import unittest

from vortex.Tuple import TUPLE_TYPES_BY_NAME, TupleHash
from vortex.test.TestTuple import TestSubTuple, TestTuple


def makeTupleA():
    tuple_ = TestTuple()
    tuple_.aBoolTrue = True
    tuple_.aBoolFalse = False
    tuple_.aDate = datetime.datetime(2010, 4, 7, 2, 33, 19, 666)
    tuple_.aFloat = 2.56
    tuple_.aInt = 1231231
    tuple_.aString = "test string from 345345345@$#%#$%#$%#$%#"
    tuple_.aSet = ["x", 2, False]  # Sets are restored as Lists
    tuple_.aList = ["y", 3, True, 3.3]
    tuple_.aDict = {
        "a": "test str",  # Key strings are loaded as unicode
        1: 1.234,
    }

    subTuple = TestSubTuple()
    subTuple.subInt = 2048
    tuple_.aSubTuple = subTuple
    #
    tuple_.aListOfSubTuples = []
    for x in range(2):
        subTuple = TestSubTuple()
        subTuple.subInt = x
        tuple_.aListOfSubTuples.append(subTuple)

    return tuple_


class TuplePyTest(unittest.TestCase):
    PERFORMANCE_ITERATIONS = 10000

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testTupleToFromJson(self):
        # Create tuple
        origTuple = makeTupleA()

        # To Json
        jsonDictStr = json.dumps(origTuple.toJsonDict())
        deserialisedTuple = TestTuple().fromJsonDict(json.loads(jsonDictStr))
        # print xmlStr

        self.assertEqual(TupleHash(origTuple), TupleHash(origTuple))
        self.assertEqual(
            TupleHash(deserialisedTuple), TupleHash(deserialisedTuple)
        )

        # print TupleHash(origTuple)._key()
        # print TupleHash(deserialisedTuple)._key()

        self.assertEqual(
            TupleHash(origTuple),
            TupleHash(deserialisedTuple),
            "Tuple serialise -> deserialise error",
        )

    def testTupleToFromJsonPerformanceMulti(self):
        for _ in range(self.PERFORMANCE_ITERATIONS):
            origTuple = makeTupleA()
            jsonDictStr = json.dumps(origTuple.toJsonDict())
            TestTuple().fromJsonDict(json.loads(jsonDictStr))

    def testTupleToFromJsonPerformanceSingle(self):
        origTuple = makeTupleA()
        origTuple.aListOfSubTuples = [
            makeTupleA() for _ in range(self.PERFORMANCE_ITERATIONS)
        ]
        jsonDictStr = json.dumps(origTuple.toJsonDict())
        TestTuple().fromJsonDict(json.loads(jsonDictStr))

    def testTupleHash(self):
        hashA1 = TupleHash(makeTupleA())
        hashA2 = TupleHash(makeTupleA())
        print("hashA1 = %s" % hashA1.__hash__())
        print("hashA2 = %s" % hashA2.__hash__())

        hashB1 = TupleHash(makeTupleA())
        hashB1.tupl.aString = "Not the same as the others"
        print("hashB1 = %s" % hashB1.__hash__())

        hashA1.__hash__()
        self.assertTrue(
            hashA1.__hash__() == hashA1.__hash__(),
            "TupleHash A1.__hash__() == A1.__hash__()",
        )

        self.assertTrue(hashA1 == hashA2, "TupleHash __eq__ fail, A1 == A2")
        self.assertFalse(hashA1 != hashA2, "TupleHash __ne__ fail, A1 != A2")
        self.assertFalse(hashA1 == hashB1, "TupleHash __eq__ fail, A1 == B1")
        self.assertTrue(hashA1 != hashB1, "TupleHash __ne__ fail, A1 != B1")

        self.assertTrue(
            hashA1 in {hashA2: None}, "TupleHash in fail, A1 in {A2:None}"
        )
        self.assertFalse(
            hashA1 not in {hashA2: None},
            "TupleHash in fail, A1 not in {A2:None}",
        )
        self.assertTrue(
            hashA1 not in {hashB1: None},
            "TupleHash in fail, A1 not in {B1:None}",
        )
        self.assertFalse(
            hashA1 in {hashB1: None}, "TupleHash in fail, A1 in {B1:None}"
        )

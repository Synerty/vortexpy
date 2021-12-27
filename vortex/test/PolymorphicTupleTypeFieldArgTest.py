import logging
import unittest

from vortex.Tuple import (
    Tuple,
    addTupleType,
    TupleField,
    PolymorphicTupleTypeFieldArg,
    TupleHash,
)

logger = logging.getLogger(__name__)


@addTupleType
class Parent(Tuple):
    __tupleType__ = "com.synerty.Tuples.Parent"
    __tupleArgs__ = (PolymorphicTupleTypeFieldArg("concreteType"),)

    aStr: str = TupleField()
    concreteType: str = TupleField()


@addTupleType
class Child(Parent):
    __tupleType__ = "com.synerty.Tuples.Child"

    aInt: int = TupleField()


@addTupleType
class Another(Tuple):
    __tupleType__ = "com.synerty.Tuples.Another"

    aInt: int = TupleField()
    concreteType: str = TupleField()


class PolymorphicTupleTypeFieldArgTest(unittest.TestCase):
    def testParentTupleToFromJSON(self):
        parent1 = Parent(aStr="Test", concreteType=Parent.tupleType())
        parent2 = Parent(aStr="Test", concreteType=Parent.tupleType())
        parent3 = Parent(aStr="Test 1", concreteType=Parent.tupleType())

        self.assertEqual(TupleHash(parent1), TupleHash(parent2))
        self.assertNotEqual(TupleHash(parent1), TupleHash(parent3))

    def testChildFieldsInToJsonDict(self):
        data = dict(aStr="Test", aInt=1, concreteType=Child.tupleType())
        child = Child(**data)

        self.assertEqual(data, child.tupleToRestfulJsonDict())

    def testChildTupleToFromJSON(self):
        # Construct Child through Child
        child1 = Child(aStr="Test", aInt=1, concreteType=Child.tupleType())

        # Construct Child through Parent
        child2 = Tuple.restfulJsonDictToTupleWithValidation(
            child1.tupleToRestfulJsonDict(), Parent
        )

        self.assertEqual(TupleHash(child1), TupleHash(child2))

        # Parent should not match Child
        parent1 = Parent(aStr="Test", concreteType=Parent.tupleType())
        self.assertNotEqual(TupleHash(child1), TupleHash(parent1))

        # Parent should only be able to init its children
        another1 = Another(aInt=1, concreteType=Another.tupleType())
        with self.assertRaises(TypeError):
            Tuple.restfulJsonDictToTupleWithValidation(
                another1.tupleToRestfulJsonDict(), Parent
            )

"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
from Tuple import TupleField, Tuple, addTupleType


@addTupleType
class TestTuple(Tuple):
    '''
    classdocs
    '''
    __tupleType__ = 'rapui.synerty.TestTuple'
    aInt = TupleField()
    aFloat = TupleField()
    aString = TupleField()
    aDate = TupleField()
    aBoolTrue = TupleField()
    aBoolFalse = TupleField()
    aSet = TupleField()
    aList = TupleField()
    aDict = TupleField()
    aSubTuple = TupleField()
    aListOfSubTuples = TupleField()


@addTupleType
class TestSubTuple(Tuple):
    ''' Test Sub Tuple
    This tuple will test if we can have tuples in tuples and still serialise and
    deserialise them.
    '''
    __tupleType__ = 'rapui.synerty.TestSubTuple'
    subInt = TupleField()

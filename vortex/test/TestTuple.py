'''
 *
 *  Copyright Synerty Pty Ltd 2013
 *
 *  This software is proprietary, you are not free to copy
 *  or redistribute this code in any format.
 *
 *  All rights to this software are reserved by
 *  Synerty Pty Ltd
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
 *
'''
import logging

from vortex.Tuple import TupleField, Tuple, addTupleType

logger = logging.getLogger(__name__)

@addTupleType
class TestTuple(Tuple):
    '''
    classdocs
    '''
    __tupleType__ = 'synerty.vortex.TestTuple'
    aInt = TupleField()
    aFloat = TupleField()
    aString = TupleField()
    aStrWithUnicode = TupleField()
    aDate = TupleField()
    aBoolTrue = TupleField()
    aBoolFalse = TupleField()
    aSet = TupleField()
    aList = TupleField()
    aDict = TupleField()
    aSubTuple = TupleField()
    aListOfSubTuples = TupleField()

    def __init__(self, *args, **kwargs):
        # logger.debug("TestTuple constructed")
        Tuple.__init__(self, *args, **kwargs)



@addTupleType
class TestSubTuple(Tuple):
    ''' Test Sub Tuple
    This tuple will test if we can have tuples in tuples and still serialise and
    deserialise them.
    '''
    __tupleType__ = 'rapui.synerty.TestSubTuple'
    subInt = TupleField()
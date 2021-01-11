"""
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
"""
import logging
from datetime import datetime
from typing import Dict
from typing import List
from typing import Union

from vortex.Tuple import Tuple
from vortex.Tuple import TupleField
from vortex.Tuple import addTupleType

logger = logging.getLogger(__name__)


@addTupleType
class TestTuple(Tuple):
    """
    classdocs
    """

    __tupleType__ = "synerty.vortex.TestTuple"
    aInt: bool = TupleField()
    aFloat: float = TupleField()
    aString: str = TupleField()
    aStrWithUnicode: str = TupleField()
    aDate: datetime = TupleField()
    aBoolTrue: bool = TupleField()
    aBoolFalse: bool = TupleField()
    aSet: List[Union[str, int, bool]] = TupleField()
    aList: List[Union[str, int, bool, float]] = TupleField()
    aDict: Dict[Union[str, int], Union[str, float]] = TupleField()
    aSubTuple: "TestSubTuple" = TupleField()
    aListOfSubTuples: List["TestSubTuple"] = TupleField()

    def __init__(self, *args, **kwargs):
        # logger.debug("TestTuple constructed")
        Tuple.__init__(self, *args, **kwargs)


@addTupleType
class TestSubTuple(Tuple):
    """Test Sub Tuple
    This tuple will test if we can have tuples in tuples and still serialise and
    deserialise them.
    """

    __tupleType__ = "rapui.synerty.TestSubTuple"
    subInt: int = TupleField()

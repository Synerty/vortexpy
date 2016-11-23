"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
from .Tuple import Tuple, TupleField, addTupleType


@addTupleType
class DataWrapTuple(Tuple):
    ''' Data Wrap Tuple

    This tuple wraps a peice of data for use in RapUI.

    Attributes:
      name         What ever you want to use it for
      data         The data

    '''

    __tupleType__ = 'c.s.r.datawraptuple'

    name = TupleField(None)
    data = TupleField(None)

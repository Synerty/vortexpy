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

from vortex.Tuple import TupleField, Tuple, addTupleType


@addTupleType
class PerformTestActionTuple(Tuple):
    """Perform Test Action Tuple

    This tuple is used for testing the action code.

    """

    __tupleType__ = "synerty.vortex.PerformTestActionTuple"
    actionDataInt = TupleField(typingType=int)
    actionDataUnicode = TupleField(typingType=str)
    failProcessing = TupleField(typingType=bool)

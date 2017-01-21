"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
from datetime import datetime
from typing import Dict, List, Any

from vortex.Tuple import Tuple, addTupleType, TupleField
from vortex.TupleSelector import TupleSelector


@addTupleType
class TupleAction(Tuple):
    __tupleType__ = "vortex.TupleAction"

    uuid = TupleField(comment="Uniquely generated id for this action",
                      typingType=str)
    dateTime = TupleField(comment="The datetime this action was created",
                          typingType=datetime)
    tupleSelector = TupleField(comment="The tuple selector for this action",
                               typingType=TupleSelector)
    tupleChanges = TupleField(comment="An array of {old:v,new:v} dicts for the changes",
                              typingType=List[Dict])
    data = TupleField(comment="Optional data for the update",
                      typingType=str)

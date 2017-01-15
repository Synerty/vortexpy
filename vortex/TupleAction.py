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
    tupleSelector = TupleField(comment="The tuple selector for this action",
                               typingType=TupleSelector)
    dateTime = TupleField(comment="The datetime this action was created",
                          typingType=datetime)
    changes = TupleField(comment="An array of {old:v,new:v} dicts for the changes",
                          typingType=List[Dict[str:Any]])
    actionKey = TupleField(comment="OR an action key describing this action",
                          typingType=str)


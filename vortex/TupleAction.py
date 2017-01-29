"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import uuid
from abc import ABCMeta
from datetime import datetime, timezone
from typing import Dict, List

from vortex.Tuple import Tuple, addTupleType, TupleField
from vortex.TupleSelector import TupleSelector


class TupleActionABC(Tuple):
    uuid = TupleField(comment="Uniquely generated id for this action",
                      typingType=str)
    dateTime = TupleField(comment="The datetime this action was created",
                          typingType=datetime)

    def __init__(self, **kwargs):
        if not self.__tupleType__:
            raise NotImplementedError(
                "TupleActionABC can not be instantiated,"
                " please inherit it and implement a tuple")

        Tuple.__init__(self, **kwargs)

        if not "uuid" in kwargs:
            self.uuid = str(uuid.uuid1())  # Based on the host, and the time

        if not "dateTime" in kwargs:
            self.dateTime = datetime.utcnow()


@addTupleType
class TupleGenericAction(TupleActionABC):
    """ Tuple Generic Action

    This is a generic action, to be used when the implementor doesn't want to implement
    concrete classes for each action type.

    """
    __tupleType__ = "vortex.TupleGenericAction"

    key = TupleField(comment="An optional key for this action",
                     typingType=str)

    data = TupleField(comment="Optional data for the update",
                      typingType=str)

@addTupleType
class TupleUpdateAction(TupleActionABC):
    __tupleType__ = "vortex.TupleUpdateAction"

    tupleSelector = TupleField(comment="The tuple selector for this action",
                               typingType=TupleSelector)
    tupleChanges = TupleField(comment="An array of {old:v,new:v} dicts for the changes",
                              typingType=List[Dict])

"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import json
from typing import Any, Dict, Optional

from vortex.Tuple import Tuple, addTupleType, TupleField


@addTupleType
class TupleSelector(Tuple):
    __tupleType__ = "vortex.TupleSelector"
    __slots__ = ["name", "selector"]

    # name: str = TupleField(comment="The tuple name this selector is for")
    # selector: Dict[str, Any] = TupleField(comment="The values to select")

    def __init__(
        self, name: Optional[str] = None, selector: Optional[Dict] = None
    ) -> None:
        Tuple.__init__(self)
        self.name = name
        self.selector = selector if selector else {}

    def __eq__(x, y):
        return x.toJsonStr() == y.toJsonStr()

    def __hash__(self):
        return hash(self.toJsonStr())

    def toJsonStr(self) -> str:
        """To Json Str

        This method dumps the c{TupleSelector} data to a json string.

        It sorts the dict keys and
        """
        fieldJsonDict = self.toJsonField(self.selector)
        return json.dumps(
            {"name": self.name, "selector": fieldJsonDict}, sort_keys=True
        )

    @classmethod
    def fromJsonStr(self, jsonStr: str) -> "TupleSelector":
        """From Json Str

        This method creates a new c{TupleSelector} from the ordered json string dumped
        from .toJsonStr

        """
        data = json.loads(jsonStr)
        newTs = TupleSelector(name=data["name"], selector={})
        newTs.selector = newTs.fromJsonField(data["selector"])
        return newTs

    def __repr__(self):
        return self.toJsonStr()

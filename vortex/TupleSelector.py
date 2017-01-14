"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import json

from vortex.Tuple import Tuple, addTupleType, TupleField


@addTupleType
class TupleSelector(Tuple):
    __tupleType__ = "vortex.TupleSelector"

    name = TupleField(comment="The tuple name this selector is for")
    selector = TupleField(comment="The values to select")

    def __init__(self, name: str = None, selector: dict = None):
        self.name = name
        self.selector = selector if selector else {}

    def __eq__(x, y):
        return x.toJsonStr() == y.toJsonStr()

    def __hash__(self):
        return hash(self.toJsonStr())

    def toJsonStr(self) -> str:
        """ To Json Str

        This method dumps the c{TupleSelector} data to a json string.

        It sorts the dict keys and
        """
        return json.dumps({'name': self.name,
                           'selector': self.selector}, sort_keys=True)

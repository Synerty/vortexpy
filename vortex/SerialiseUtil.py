"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import decimal
import logging
from datetime import datetime, date

from base64 import b64encode, b64decode
from collections import defaultdict
from sqlalchemy.orm.collections import InstrumentedList

logger = logging.getLogger(__name__)

try:
    from geoalchemy2.elements import WKBElement

except ImportError:
    class WKBElement:
        def __init__(self):
            raise NotImplementedError("")

try:
    from geoalchemy2.elements import WKBElement

except ImportError:
    class WKBElement:
        def __init__(self):
            raise NotImplementedError("")

###############################################################################
# Utility functions
###############################################################################

T_RAPUI_TUPLE = 'rt'
T_RAPUI_PAYLOAD = 'rp'
T_GENERIC_CLASS = 'gen'  # NOT SUPPORTED
T_FLOAT = 'float'
T_INT = 'int'
T_STR = 'str'
T_BYTES = 'bytes'
T_BOOL = 'bool'
T_DATETIME = 'datetime'
T_DICT = 'dict'
T_LIST = 'list'
T_GEOM = 'geom'

V_NULL = 'null'
V_TRUE = '1'
V_FALSE = '0'

ISO8601_ORA = 'YYYY-MM-DD HH24:MI:SS.FF6'
ISO8601 = '%Y-%m-%d %H:%M:%S.%f%z'
ISO8601_NOTZ = '%Y-%m-%d %H:%M:%S.%f'

TYPE_MAP_PY_TO_RAPUI = {decimal.Decimal: T_FLOAT,
                        float: T_FLOAT,
                        str: T_STR,
                        bytes: T_BYTES,
                        int: T_INT,
                        bool: T_BOOL,
                        datetime: T_DATETIME,
                        date: T_DATETIME,
                        dict: T_DICT,
                        defaultdict: T_DICT,
                        list: T_LIST,
                        set: T_LIST,
                        tuple: T_LIST,
                        WKBElement: T_GEOM,
                        InstrumentedList: T_LIST
                        }


def className(o):
    cls = o if o.__class__ == type else o.__class__
    return str(cls).split("'")[1]


def convertFromShape(shapelyShape):
    from shapely.geometry.polygon import Polygon
    if isinstance(shapelyShape, Polygon):
        coords = shapelyShape.exterior.coords
    else:
        coords = shapelyShape.coords
    return [{'x': i[0], 'y': i[1]} for i in coords]


def convertFromWkbElement(value):
    from geoalchemy2.shape import to_shape
    return convertFromShape(to_shape(value))


def constructGeom(val):
    raise NotImplementedError("Receiving WKBElements is not implemented")


TYPE_MAP_RAPUI_TO_PY = {T_FLOAT: float,
                        T_STR: str,
                        T_BYTES: bytes,
                        T_INT: int,
                        T_BOOL: bool,
                        T_DATETIME: datetime,
                        T_DICT: dict,
                        T_LIST: list,
                        T_GEOM: constructGeom
                        }


def decimalToStr(dec):
    """Return string representation of the number in scientific notation.

    Captures all of the information in the underlying representation.
    """

    if str(dec) == '0E-10':
        return '0'

    sign = ['', '-'][dec._sign]
    if dec._is_special:
        if dec._exp == 'F':
            return sign + 'Infinity'
        elif dec._exp == 'n':
            return sign + 'NaN' + dec._int
        else:  # dec._exp == 'N'
            return sign + 'sNaN' + dec._int

    # number of digits of dec._int to left of decimal point
    leftdigits = dec._exp + len(dec._int)

    # no exponent required
    dotplace = leftdigits

    if dotplace <= 0:
        intpart = '0'
        fracpart = '.' + '0' * (-dotplace) + dec._int
    elif dotplace >= len(dec._int):
        intpart = dec._int + '0' * (dotplace - len(dec._int))
        fracpart = ''
    else:
        intpart = dec._int[:dotplace]
        fracpart = '.' + dec._int[dotplace:]

    if leftdigits == dotplace:
        exp = ''
    else:
        exp = "E%+d" % (leftdigits - dotplace)

    s = (sign + intpart + fracpart + exp)
    if '.' in s:
        s = s.rstrip('0').rstrip('.')
    assert (len(s))
    return s


def toStr(obj) -> str:
    if isinstance(obj, decimal.Decimal):
        return decimalToStr(obj)

    if isinstance(obj, datetime):
        return obj.strftime(ISO8601)

    if isinstance(obj, bool):
        return V_TRUE if obj else V_FALSE

    if isinstance(obj, bytes):
        return b64encode(obj).decode() # to make it a str

    if isinstance(obj, str):
        return obj

    try:
        return str(obj)
    except:
        logger.critical("Failed to convert the following to string")
        logger.critical(obj)
        raise


def fromStr(val: str, typeName: str):
    if typeName == T_DATETIME:
        if len(val) <= len('2017-06-06 09:40:37.097068'):
            return datetime.strptime(val, ISO8601_NOTZ)
        return datetime.strptime(val, ISO8601)

    if typeName == T_BOOL:
        return val == V_TRUE

    if typeName == T_BYTES:
        return b64decode(val)

    if typeName in (T_STR):
        return val

    pyVal = fromRapuiType(typeName)(val)

    if typeName == T_FLOAT and pyVal.is_integer():
        pyVal = int(pyVal)

    return pyVal


def fromRapuiType(typeName: str):
    return TYPE_MAP_RAPUI_TO_PY[typeName]


def toRapuiType(value):
    if value is None:
        return V_NULL

    from .Tuple import Tuple
    if isinstance(value, Tuple):
        return T_RAPUI_TUPLE

    from .Payload import Payload
    if isinstance(value, Payload):
        return T_RAPUI_PAYLOAD

    valueClass = value.__class__

    return TYPE_MAP_PY_TO_RAPUI[valueClass]

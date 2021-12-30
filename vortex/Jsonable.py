"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
import traceback
import typing
from copy import copy, deepcopy
from json import JSONEncoder
from typing import List
from _collections import defaultdict

logger = logging.getLogger(__name__)

from .SerialiseUtil import *

###############################################################################
# JSON Constants
###############################################################################

# Globally mess with the json dump separators for more compact json
JSONEncoder.item_separator = ","
JSONEncoder.key_separator = ":"


class Jsonable(object):
    """Xmlable
    Inherit from this class if you want to serialise to XML.
    """

    __fieldNames__: List[str] = None
    __rapuiSerialiseType__ = T_GENERIC_CLASS

    __rawJonableFields__: List[str] = None

    JSON_CLASS_TYPE = "_ct"
    JSON_CLASS = "_c"
    JSON_TUPLE_TYPE = "_c"
    JSON_FIELD_TYPE = "_ft"
    JSON_FIELD_DATA = "_fd"

    __memoryLoggingRefs = None
    __memoryLoggingEnabled = False

    @classmethod
    def setupMemoryLogging(cls) -> None:
        cls.__memoryLoggingRefs = defaultdict(lambda: 0)
        cls.__memoryLoggingEnabled = True

    @classmethod
    def memoryLoggingDump(
        cls, top=10, over=100
    ) -> List[typing.Tuple[str, int]]:
        if not Jsonable.__memoryLoggingRefs:
            return []

        data = sorted(
            Jsonable.__memoryLoggingRefs.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        return list(filter(lambda x: x[1] >= over, data))[:top]

    def __memLoggingKey(self):
        if getattr(self, "__rapuiSerialiseType__") == T_RAPUI_TUPLE:
            key = "Tuple: " + getattr(self, "__tupleType__")

        elif getattr(self, "__rapuiSerialiseType__") == T_RAPUI_PAYLOAD:
            assert hasattr(self, "filt"), "Payload is missing filt"
            if "key" in self.filt:
                key = "Payload: key=" + self.filt["key"]
            else:
                key = "Payload: " + str(self.filt)

        else:
            key = "Jsonable: " + getattr(self, "__rapuiSerialiseType__")

        return key

    def __init__(self):
        if Jsonable.__memoryLoggingEnabled:
            Jsonable.__memoryLoggingRefs[self.__memLoggingKey()] += 1

    def __del__(self):
        if Jsonable.__memoryLoggingEnabled:
            Jsonable.__memoryLoggingRefs[self.__memLoggingKey()] -= 1

    def __isRawJsonableField(self, name: str) -> bool:
        if not (name and self.__rawJonableFields__):
            return False
        return name in self.__rawJonableFields__

    def toJsonField(self, value, jsonDict=None, name=None):
        """To Json Field"""
        from .Tuple import TupleField

        if isinstance(value, TupleField):
            value = (
                None if value.defaultValue == None else copy(value.defaultValue)
            )

        try:
            valueType = V_NULL if value is None else toRapuiType(value)
        except KeyError as e:
            raise KeyError(
                "%s field name is %s, type is %s", e, name, value.__class__
            )

        # Payloads and Tuples
        if self.__isRawJsonableField(name):
            # This sho
            convertedValue = deepcopy(value)

        elif isinstance(value, Jsonable):
            convertedValue = value.toJsonDict()

        elif isinstance(value, dict):
            convertedValue = {}
            for key, value in list(value.items()):
                self.toJsonField(value, convertedValue, key)

        elif isinstance(value, (set, list, tuple)):
            convertedValue = []
            for val in value:
                convertedValue.append(self.toJsonField(val))

        # Decimals are detected as floats for some reason, so convert it here
        elif valueType == T_FLOAT:
            convertedValue = float(value)

        elif valueType in (T_INT, T_BOOL, T_STR):
            convertedValue = value

        elif valueType == V_NULL:
            convertedValue = None

        else:
            convertedValue = toStr(value)

        # Non standard values need a dict to store their value type attributes
        # Create a sub dict that contains the value and type
        if valueType not in (
            T_FLOAT,
            T_STR,
            V_NULL,
            T_BOOL,
            T_LIST,
            T_DICT,
        ) and not isinstance(value, Jsonable):
            convertedValue = {
                Jsonable.JSON_FIELD_TYPE: valueType,
                Jsonable.JSON_FIELD_DATA: convertedValue,
            }

        if name and jsonDict is not None:
            # Now assign the value and it's data type if applicable
            if not toRapuiType(name) in (T_STR, T_INT, T_FLOAT):
                raise Exception(
                    "name=%s, type=%s, is not an allowed dict key",
                    name,
                    toRapuiType(name),
                )

            jsonDict[name] = convertedValue

        return convertedValue

    # -----------------------------------------------------------------------------
    def fromJsonField(self, value, valueType=None):
        # Single Value
        if (
            valueType == V_NULL or value is None
        ):  # V_NULL will never be set in toJsonField
            return None

        if valueType == T_INT:
            return int(value)

        if isinstance(value, dict) and Jsonable.JSON_CLASS_TYPE in value:
            valueType = value[Jsonable.JSON_CLASS_TYPE]

        # JSON handles these types natively,
        # if there is no type then these are the right types
        if valueType is None:
            valueType = toRapuiType(value)
            if valueType in (T_BOOL, T_FLOAT, T_INT, T_STR):
                return value

        # Non standard values need a dict to store their value type attributes, decode these
        if isinstance(value, dict) and Jsonable.JSON_FIELD_TYPE in value:
            return self.fromJsonField(
                value[Jsonable.JSON_FIELD_DATA], value[Jsonable.JSON_FIELD_TYPE]
            )

        # Tuple
        if valueType == T_RAPUI_TUPLE:
            tupleType = value[Jsonable.JSON_TUPLE_TYPE]

            from .Tuple import TUPLE_TYPES_BY_NAME

            if not tupleType in TUPLE_TYPES_BY_NAME:
                raise Exception(
                    "Tuple type |%s| not registered within this program.",
                    tupleType,
                )

            try:
                from .Tuple import TUPLE_TYPES_BY_NAME

                return TUPLE_TYPES_BY_NAME[tupleType]().fromJsonDict(value)

            except Exception as e:
                logger.critical(traceback.format_exc())
                raise Exception("%s for tuple type %s" % (str(e), tupleType))

        # Handle the case of payloads within payloads
        if valueType == T_RAPUI_PAYLOAD:
            from .Payload import Payload

            return Payload().fromJsonDict(value)

        if valueType == T_RAPUI_PAYLOAD_ENVELOPE:
            from .PayloadEnvelope import PayloadEnvelope

            return PayloadEnvelope().fromJsonDict(value)

        if valueType == T_GENERIC_CLASS:
            # OTHER JSONABLES GO HERE, INSPECT THE tuple_TYPE
            # jsonClass = value[JSON_CLASS]
            raise NotImplementedError()
            # import importlib
            # MyClass = getattr(importlib.import_module("module.submodule"), "Klass")
            # instance = MyClass()

        if valueType == T_DICT:
            # Dict
            restoredDict = {}
            for subName, subVal in list(value.items()):
                try:
                    subNamStr = subName
                    subName = float(subNamStr)
                    subName = int(subNamStr)
                except ValueError:
                    pass
                restoredDict[subName] = self.fromJsonField(subVal)

            return restoredDict

        if valueType == T_LIST:
            restoredList = []
            for subVal in value:
                restoredList.append(self.fromJsonField(subVal))

            return restoredList

        return fromStr(value, valueType)

    def toJsonDict(self):

        fieldNames = self.__fieldNames__

        jsonDict = {Jsonable.JSON_CLASS_TYPE: self.__rapuiSerialiseType__}

        if hasattr(self, "tupleName"):
            jsonDict[Jsonable.JSON_TUPLE_TYPE] = self.tupleName()

        else:
            jsonDict[Jsonable.JSON_CLASS] = className(self)

        for name in fieldNames:
            data = getattr(self, name)
            self.toJsonField(data, jsonDict, name)

        return jsonDict

    def fromJsonDict(self, jsonDict):
        """From Xml
        Returns and instance of this object populated with data from the json dict
        """

        # Use the fromJsonField code to convert all the values
        for name, value in list(jsonDict.items()):
            if name.startswith("_"):
                continue

            if self.__isRawJsonableField(name):
                setattr(self, name, value)
            else:
                setattr(self, name, self.fromJsonField(value))

        return self

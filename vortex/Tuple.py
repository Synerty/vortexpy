"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import inspect
import json
import re
from abc import ABCMeta, abstractmethod, ABC
from copy import deepcopy
from datetime import datetime
from time import strptime
from typing import Dict, Optional, Annotated, Any
from typing import List
from typing import Type
from typing import get_args
from typing import get_origin
from typing import get_type_hints

from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.properties import RelationshipProperty
from sqlalchemy.sql.schema import Sequence

from vortex.SerialiseUtil import ISO8601_REGEXP
from .Jsonable import Jsonable
from .SerialiseUtil import ISO8601
from .SerialiseUtil import T_RAPUI_TUPLE
from .SerialiseUtil import WKBElement
from .SerialiseUtil import convertFromWkbElement

TUPLE_TYPES = []
TUPLE_TYPES_BY_NAME: Dict[str, "Tuple"] = {}
TUPLE_TYPES_BY_SHORT_NAME: Dict[str, "Tuple"] = {}

JSON_EXCLUDE = "jsonExclude"


def registeredTupleNames():
    return list(TUPLE_TYPES_BY_NAME.keys())


def tupleForTupleName(tupleName):
    return TUPLE_TYPES_BY_NAME[tupleName]


def getSqlaRelationshipFieldNames(cls):
    objDict = _getTupleInternalDict(cls)

    # Tell the tuple fields of their variable name
    fields = []
    for name, value in objDict.items():
        if isinstance(value, InstrumentedAttribute) and isinstance(
            value.comparator.prop, RelationshipProperty
        ):
            fields.append(value)

    return fields


def addTupleType(cls):
    if cls.tupleName() is None:
        raise Exception(
            "You must set __tupleType__ to a unique string for this tuple"
        )

    if not cls.tupleName():
        cls.__tupleType__ = "%s.%s" % (cls.__module__, cls.__name__)

    tupleType = cls.tupleName()
    tupleTypeShort = cls.__tupleTypeShort__
    if tupleType in TUPLE_TYPES_BY_NAME or tupleType is None:
        raise Exception(
            "Tuple name is None or is already in registered.\n"
            "Tuple name is %s" % tupleType
        )

    if tupleTypeShort:
        if tupleTypeShort in TUPLE_TYPES_BY_SHORT_NAME:
            raise Exception(
                "Tuple short name is already registered.\n"
                "Tuple short name is %s" % tupleTypeShort
            )

        TUPLE_TYPES_BY_SHORT_NAME[tupleTypeShort] = cls

    # Setup the lookups
    TUPLE_TYPES.append(cls)
    TUPLE_TYPES_BY_NAME[tupleType] = cls

    def underscoreException():
        raise Exception(
            "TupleFields can not start with an underscore. "
            "They can potentially clash with inner tuple workings"
            " if they do."
        )

    # This is required because SQLAlchemy seems to alter the inhertance.
    # OR __dict__ just doesn't contian attributes defined in the base classes
    # If we inherit from a tuple that has a tuple field, that field should be included
    def getBaseClassTupleFields(cls, objDict):
        # Skip these types
        if cls in (object, Tuple, Jsonable):
            return

        for name, value in cls.__dict__.items():
            # SQLAlchemy takes care of it's own fields.
            if isinstance(value, TupleField):
                objDict[name] = value

    objDict = {}
    for baseCls in reversed(inspect.getmro(cls)[1:]):  # The first item is us
        getBaseClassTupleFields(baseCls, objDict)
    objDict.update(cls.__dict__)

    # Tell the tuple fields of their variable name
    fields = []
    shortFieldNames = set()
    for name, value in objDict.items():

        if isinstance(value, TupleField):
            shortName = value.shortName if value.shortName else name

        elif isinstance(value, InstrumentedAttribute):
            if isinstance(value.comparator.prop, RelationshipProperty):
                continue

            shortName = value.doc if value.doc else name

        else:
            continue

        if name.startswith("_"):
            underscoreException()

        if shortName.startswith("_"):
            underscoreException()

        value.name = name
        fields.append(name)

        if shortName != JSON_EXCLUDE:
            if shortName in shortFieldNames:
                raise Exception(
                    "TupleField %s short name %s is alread registered"
                    % (name, shortName)
                )

            shortFieldNames.add(shortName)

    hasSlots = hasattr(cls, "__slots__")
    hasFieldNames = cls.__fieldNames__ is not None

    if hasSlots and hasFieldNames:
        raise Exception(
            "Only one of __slots__ or __fieldNames__ can be defined"
            " but not both"
        )

    # If field names already exist, then work off these
    if hasFieldNames:
        fields = list(set(fields + list(cls.__fieldNames__)))

        # Just check that the field names are defined.
        for fieldName in fields:
            if not hasattr(cls, fieldName):
                raise Exception(
                    "Tuple %s doesn't have field %s" % (tupleType, fieldName)
                )

    # If field names already exist, then work off these
    if hasSlots:
        fields = list(set(fields + list(cls.__slots__)))

    # Sort and set the field names
    fields.sort()
    cls.__fieldNames__ = fields

    __mapShortFieldNames(cls)

    return cls


def _getTupleInternalDict(cls):
    """Get Tuple Internal Dict

    This is required because SQLAlchemy seems to alter the inhertance.
    OR __dict__ just doesn't contian attributes defined in the base classes
    If we inherit from a tuple that has a tuple field, that field should be included
    """

    def getBaseClassTupleFields(cls, objDict):
        # Skip these types
        if cls in (object, Tuple, Jsonable):
            return

        for name, value in cls.__dict__.items():
            # SQLAlchemy takes care of it's own fields.
            if isinstance(value, TupleField):
                objDict[name] = value

    objDict = {}
    for baseCls in reversed(inspect.getmro(cls)[1:]):  # The first item is us
        getBaseClassTupleFields(baseCls, objDict)
    objDict.update(cls.__dict__)
    return objDict


def __mapShortFieldNames(cls):
    fieldMap = {}
    for normalName in cls.__fieldNames__:
        field = getattr(cls, normalName, None)
        if not field:
            continue

        if isinstance(field, TupleField):
            if field.jsonExclude:
                continue

            shortName = field.shortName if field.shortName else field.name

        elif isinstance(field, InstrumentedAttribute):
            if isinstance(field.comparator.prop, RelationshipProperty):
                continue

            shortName = field.doc if field.doc else field.name

        else:
            continue

        # Underscore means skip
        if shortName == JSON_EXCLUDE:
            continue

        fieldMap[shortName] = normalName

    cls.__shortFieldNamesMap__ = fieldMap


def removeTuplesForPackage(packageName):
    tupleNames = [
        cls.tupleName()
        for cls in TUPLE_TYPES
        if cls.__name__.startswith("%s." % packageName)
    ]

    removeTuplesForTupleNames(tupleNames)


def removeTuplesForTupleNames(tupleNames):
    global TUPLE_TYPES, TUPLE_TYPES_BY_NAME, TUPLE_TYPES_BY_SHORT_NAME

    tupleNames = set(tupleNames)

    def filt(cls):
        return cls.tupleName() not in tupleNames

    tupleShortNames = [
        cls.__tupleTypeShort__ for cls in TUPLE_TYPES if filt(cls)
    ]

    # Remove from tuple types
    TUPLE_TYPES = list(filter(filt, TUPLE_TYPES))

    # Remove from tuple types by name
    for tupleName in tupleNames:
        if tupleName in TUPLE_TYPES_BY_NAME:
            del TUPLE_TYPES_BY_NAME[tupleName]

    # Remove from tuple types by name
    for tupleShortName in tupleShortNames:
        if tupleShortName in TUPLE_TYPES_BY_SHORT_NAME:
            del TUPLE_TYPES_BY_SHORT_NAME[tupleShortName]


class TupleField:
    class _Map:
        pass

    def __init__(
        self,
        defaultValue=None,
        typingType=None,
        comment="",
        shortName=None,
        jsonExclude=False,
    ):
        self.name = None
        self.shortName = shortName
        self.defaultValue = defaultValue
        self.typingType = typingType
        self.comment = comment
        self.jsonExclude = jsonExclude


class PolymorphicTupleTypeFieldArg:
    def __init__(self, fieldName: str):
        self.fieldName = fieldName


class _TupleToSqlaJsonMixin:
    def tupleToSqlaBulkInsertDict(self, includeNulls=True):
        insertDict = {}

        for field in self.__class__.__dict__.values():

            if not isinstance(field, InstrumentedAttribute):
                continue

            if isinstance(field.comparator.prop, RelationshipProperty):
                continue

            def convert(value):
                if isinstance(value, WKBElement):
                    return value.desc

                else:
                    return value

            value = getattr(self, field.name)
            if includeNulls or value is not None:
                insertDict[field.name] = convert(value)

        return insertDict


class TupleFieldValidatorABC(ABC):
    """Tuple Value Validator

    This class can be used to validate value ranges within a vortex.Tuple field

    """

    @abstractmethod
    def validate(self, fieldName: str, value):
        """Validate

        This method is called to validate the value.
        If the value is invalid a ValueError will be raised.

        :param fieldName: The name of the field being validated
        :param value: The value to be checked
        """


class IntTupleFieldValidator(TupleFieldValidatorABC):
    def __init__(self, low: Optional[int], high: Optional[int]):
        self._low = low
        self._high = high

    def validate(self, fieldName: str, value):
        if self._low is not None and value < self._low:
            raise ValueError(
                f"Field {fieldName}," f" Value {value} is less than {self._low}"
            )

        if self._high is not None and self._high < value:
            raise ValueError(
                f"Field {fieldName},"
                f" Value {value} is greater than {self._high}"
            )


def getConcreteClass(TupleClass, jsonDict):
    # Create concrete class for Tuple
    if not hasattr(TupleClass, "__tupleArgs__"):
        return TupleClass

    polyType = [
        p
        for p in TupleClass.__tupleArgs__
        if isinstance(p, PolymorphicTupleTypeFieldArg)
    ]
    if not polyType:
        return TupleClass

    fieldName = polyType[0].fieldName
    ConcreteTupleClass = TUPLE_TYPES_BY_NAME[jsonDict[fieldName]]

    if not issubclass(ConcreteTupleClass, TupleClass):
        raise TypeError(
            f"{ConcreteTupleClass.__name__} is not derived"
            f" from {TupleClass.__name__}"
        )

    return ConcreteTupleClass


class _TupleToPlainJsonMixin:
    def tupleToSmallJsonDict(self, includeNones=True, includeFalse=True):
        if not self.__shortFieldNamesMap__:
            raise Exception(
                "Tuple %s has no shortFieldNames defined" % self.tupleType()
            )

        return self.__tupleToJsonDict(
            includeNones=includeNones,
            includeFalse=includeFalse,
            useShortNames=True,
        )

    def tupleToRestfulJsonDict(self, includeNones=True, includeFalse=True):
        return self.__tupleToJsonDict(
            includeNones=includeNones,
            includeFalse=includeFalse,
            useShortNames=False,
        )

    def __tupleToJsonDict(
        self, includeNones=True, includeFalse=True, useShortNames=True
    ):

        if useShortNames:
            json = {
                "_tt": (
                    self.__tupleTypeShort__
                    if self.__tupleTypeShort__
                    else self.__tupleType__
                )
            }
        else:
            json = {}

        def convert(value):
            if isinstance(value, list):
                return [convert(v) for v in value]

            elif isinstance(value, dict):
                return {convert(k): convert(i) for k, i in value.items()}

            elif isinstance(value, Tuple):
                if useShortNames:
                    return value.tupleToSmallJsonDict()
                return value.tupleToRestfulJsonDict()

            elif isinstance(value, WKBElement):
                return convertFromWkbElement(value)

            elif isinstance(value, TupleField):
                return None

            elif isinstance(value, datetime):
                return value.strftime(ISO8601)

            else:
                return value

        for shortName, normalName in self.__shortFieldNamesMap__.items():
            value = convert(getattr(self, normalName))
            if value is None and not includeNones:
                continue
            if value is False and not includeFalse:
                continue
            if useShortNames:
                json[shortName] = value
            else:
                json[normalName] = value

        return json

    @staticmethod
    def smallJsonDictToTuple(jsonDict: dict) -> "Tuple":
        tupleShortType = jsonDict.get("_tt")
        if not tupleShortType:
            raise Exception(
                "Tuple.smallJsonDictToTuple: jsonDict has no _tt field"
            )

        Tuple_ = TUPLE_TYPES_BY_SHORT_NAME.get(tupleShortType)
        if not Tuple_:
            raise Exception(
                "Tuple.smallJsonDictToTuple: %s is not a registered tuple type"
                % tupleShortType
            )

        if not Tuple_.__shortFieldNamesMap__:
            raise Exception(
                "Tuple %s has no shortFieldNames defined" % Tuple.tupleType()
            )

        def convert(value):
            if value in (None, ""):
                return value

            if isinstance(value, list):
                return [convert(v) for v in value]

            if isinstance(value, dict):
                if value.get("_tt", None):
                    return Tuple.smallJsonDictToTuple(value)
                return {convert(k): convert(i) for k, i in value.items()}

            # elif isinstance(value, WKBElement):
            #     return convertFromWkbElement(value)

            if isinstance(value, str) and re.match(ISO8601_REGEXP, value):
                return strptime(value, ISO8601)

            return value

        newTuple = Tuple_()

        for shortName, normalName in Tuple_.__shortFieldNamesMap__.items():
            setattr(newTuple, normalName, convert(jsonDict.get(shortName)))

        return newTuple

    @staticmethod
    def restfulJsonDictToTupleWithValidation(
        jsonDict: dict, TupleClass: Type["Tuple"]
    ) -> "Tuple":
        """Restful JsonDict to Tuple With Validation

        This method uses the typings on the fields in a tuple to validate the
        types of data
        """

        if not get_type_hints(TupleClass):
            raise TypeError(f"{TupleClass} has no annotations")

        # Create concrete class for Tuple
        TupleClass = getConcreteClass(TupleClass, jsonDict)

        typeHints = get_type_hints(TupleClass)
        extraTypeHits = get_type_hints(TupleClass, include_extras=True)

        def getAndCheckTypeHint(fieldName):
            typeHint = typeHints.get(fieldName, None)

            if not typeHint:
                raise TypeError(
                    f"Type hint for {TupleClass} field {fieldName} is falsy"
                )

            # Check some type hints
            if get_origin(typeHint) is dict:
                keyHint, _ = get_args(typeHint)
                if keyHint is not str:
                    raise TypeError(
                        f"Type hint for {TupleClass}"
                        f" field {fieldName}"
                        f" must have a str type for the key,"
                        f" got {keyHint}"
                    )

            return typeHint

        def getExtraTypeHint(fieldName):
            extraTypeHit = extraTypeHits.get(fieldName, None)
            return extraTypeHit

        def getFieldValidatorFromExtraHint(extraTypeHint):

            origin = get_origin(extraTypeHint)

            # Check if the type hint is an Annotated
            if origin is not Annotated:
                return None

            for hint in get_args(extraTypeHint):
                if isinstance(hint, TupleFieldValidatorABC):
                    return hint

            return None

        def raiseIfNotType(
            fieldName,
            typeHint,
            value,
            validator: Optional[TupleFieldValidatorABC] = None,
        ):
            # if the field type is of the type typeHint, then we are good.
            if not get_origin(typeHint) and isinstance(value, typeHint):
                if validator:
                    validator.validate(fieldName, value)
                return

            # if the field type is of the type typeHint, then we are good.
            if isinstance(value, list) and get_origin(typeHint) is list:
                return

            # if the field type is of the type typeHint, then we are good.
            if isinstance(value, dict):
                if get_origin(typeHint) is dict:
                    return

                # If the type is a tuple and the value is a dict, that's a pass
                if hasattr(typeHint, "__tupleType__"):
                    return

            # handle the the Optional[fieldType] or Union case
            if type(value) in get_args(typeHint):
                return

            # Handle the None and Optional[fieldType] case
            if value is None and type(None) in get_args(typeHint):
                return

            raise TypeError(
                f"Parsing {TupleClass}, field {fieldName}"
                f" is not of type {typeHint}, got {type(value)} instead"
            )

        def convert(fieldName: str, value):
            typeHint = getAndCheckTypeHint(fieldName)
            extraTypeHint = getExtraTypeHint(fieldName)
            validator = getFieldValidatorFromExtraHint(extraTypeHint)

            raiseIfNotType(fieldName, typeHint, value, validator)

            if value in (None, ""):
                return value

            if isinstance(value, str) and re.match(ISO8601_REGEXP, value):
                return strptime(value, ISO8601)

            if isinstance(value, list):
                listItemTypeHint = get_args(typeHint)[0]
                listItemTypeValidator = getFieldValidatorFromExtraHint(
                    get_args(extraTypeHint)[0]
                )
                data = []
                if hasattr(listItemTypeHint, "__tupleType__"):
                    for item in value:
                        if not isinstance(item, dict):
                            raise TypeError(
                                f"Parsing {TupleClass}, field {fieldName}"
                                f" list item is not a json object to convert to "
                                f" {listItemTypeHint}, got {type(value)} instead"
                            )

                        data.append(
                            TupleClass.restfulJsonDictToTupleWithValidation(
                                item, listItemTypeHint
                            )
                        )

                else:
                    for item in value:
                        raiseIfNotType(
                            fieldName,
                            listItemTypeHint,
                            item,
                            listItemTypeValidator,
                        )
                        data.append(item)

                return data

            if isinstance(value, dict):
                # If this is just a single child tuple, import it
                if hasattr(typeHint, "__tupleType__"):
                    return TupleClass.restfulJsonDictToTupleWithValidation(
                        value, typeHint
                    )

                if not get_args(typeHint):
                    raise Exception(
                        "Typing should use `Dict[<type>,<type>]`," " not `dict`"
                    )

                keyTypeHint, valueTypeHint = get_args(typeHint)

                data = {}
                if hasattr(valueTypeHint, "__tupleType__"):
                    for key, item in value.items():
                        raiseIfNotType(fieldName, keyTypeHint, key)
                        if not isinstance(item, dict):
                            raise TypeError(
                                f"Parsing {TupleClass}, field {fieldName}"
                                f" list item is not a json object to convert to "
                                f" {valueTypeHint}, got {type(value)} instead"
                            )

                        data[
                            key
                        ] = TupleClass.restfulJsonDictToTupleWithValidation(
                            item, valueTypeHint
                        )

                else:
                    keyTypeExtraHint, valueTypeExtraHint = get_args(
                        extraTypeHint
                    )
                    keyTypeValidator = getFieldValidatorFromExtraHint(
                        keyTypeExtraHint
                    )
                    valueTypeValidator = getFieldValidatorFromExtraHint(
                        valueTypeExtraHint
                    )

                    for key, item in value.items():
                        raiseIfNotType(
                            fieldName, keyTypeHint, key, keyTypeValidator
                        )
                        raiseIfNotType(
                            fieldName, valueTypeHint, item, valueTypeValidator
                        )
                        data[key] = item  # No change

                return data

            # elif isinstance(value, WKBElement):
            #     return convertFromWkbElement(value)

            return value

        newTuple = TupleClass()

        for fieldName in TupleClass.__fieldNames__:
            setattr(
                newTuple, fieldName, convert(fieldName, jsonDict.get(fieldName))
            )

        return newTuple


class Tuple(Jsonable, _TupleToPlainJsonMixin, _TupleToSqlaJsonMixin):
    """Tuple

    This class provides rich serialisation / deserialisation support.

    The advantage over pure JSON is as follows:

    * Data structures are restored into full Tuple classes, allowing the use of
      properties, and methods on the tuples.

    * Datetimes and ints are serialised and deserialsied

    """

    #: Tuple Type, EG com.synerty.rapui.UnitTestTuple
    __tupleType__: str = None
    __tupleTypeShort__ = None
    __tupleArgs__ = ()  # Empty tuple
    __fieldNames__ = None
    __shortFieldNamesMap__ = None
    __rapuiSerialiseType__ = T_RAPUI_TUPLE

    def __init__(self, **kwargs):
        if self.__fieldNames__ is None:
            raise Exception("This tuple is missing the @addTupleType decorator")

        Jsonable.__init__(self)

        # If we're using slots, then don't use tuple fields (there arn't any anyway)
        if hasattr(self.__class__, "__slots__"):
            # noinspection PyTypeChecker
            for key in self.__class__.__slots__:
                if key in kwargs:
                    setattr(self, key, kwargs.pop(key))
                else:
                    setattr(self, key, None)

            if kwargs:
                raise KeyError(
                    "kwargs %s were passed, but tuple %s has no such fields"
                    % (", ".join(kwargs), self.__tupleType__)
                )

            return

        # Reset all the tuples.
        # We never want TupleField in an instance
        for name in self.__fieldNames__:
            tupleField = getattr(self.__class__, name)

            if isinstance(tupleField, TupleField):
                setattr(self, name, deepcopy(tupleField.defaultValue))

            elif isinstance(tupleField, InstrumentedAttribute):
                default = (
                    self.__table__.c[name].default
                    if name in self.__table__.c
                    else None
                )

                assign = (
                    default is not None
                    and getattr(self, name) is None
                    and not isinstance(default, Sequence)
                )
                if assign:
                    setattr(self, name, deepcopy(default.arg))

        # It's faster to add these at the end, rather than have logic to add one or
        # the other
        for key, val in kwargs.items():
            if not hasattr(self, key):
                raise KeyError(
                    "kwarg %s was passed, but tuple %s has no such TupleField"
                    % (key, self.__tupleType__)
                )
            setattr(self, key, val)

        # Handle setting any polymorphic field types
        for arg in self.__tupleArgs__:
            if isinstance(arg, PolymorphicTupleTypeFieldArg):
                setattr(self, arg.fieldName, self.__tupleType__)

    @classmethod
    def tupleFieldNames(cls) -> List[str]:
        return cls.__fieldNames__

    @classmethod
    def tupleName(cls):  # DEPRECIATED
        return cls.__tupleType__

    @classmethod
    def tupleType(cls):
        return cls.__tupleType__

    @classmethod
    def isSameTupleType(cls, other):
        if not hasattr(other, "__tupleType__"):
            return False
        return cls.__tupleType__ == other.__tupleType__

    def tupleClone(self):
        clone = self.__class__()
        for name in self.__fieldNames__:
            val = getattr(self, name)
            if val is not None:
                setattr(clone, name, val)

        return clone

    def _fromJson(self, jsonStr: str):
        jsonDict = json.loads(jsonStr)

        assert jsonDict[Jsonable.JSON_CLASS_TYPE] == self.__rapuiSerialiseType__
        return self.fromJsonDict(jsonDict)

    def _toJson(self) -> str:
        return json.dumps(self.toJsonDict())

    def __eq__(self, other):
        return id(self) == id(other)
        # if other == None or type(self) != type(other):
        #   return False
        #
        # # return id(self) != id(other)
        # for f in self.__fieldNames__:
        #   v1 = getattr(self, f, None)
        #   v2 = getattr(other, f, None)
        #
        #   if isinstance(v1, Tuple):
        #     if v1 is not v2:
        #       return False
        #   elif v1 != v2:
        #     return False
        #
        # return True

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        return id(self)

    def __repr__(self):
        val = lambda name: (
            getattr(self, name) if hasattr(self, name) else "AttributeError"
        )

        vals = ["type = %s," % self.tupleType()]
        vals.extend(
            ["%s = %s," % (name, val(name)) for name in self.__fieldNames__]
        )

        return "\n".join(vals)


class TupleHash(object):
    def __init__(self, tupl):
        self.tupl = tupl
        from .Payload import Payload

        assert tupl is not None
        assert isinstance(tupl, Tuple) or isinstance(tupl, Payload)

    def _key(self):
        vals = []
        if isinstance(self.tupl, Tuple):
            vals += [self.tupl.tupleName()]
        for name in self.tupl.__fieldNames__:
            val = getattr(self.tupl, name)
            if isinstance(val, dict):
                newItems = []
                for k, v in list(val.items()):
                    k = TupleHash(k) if isinstance(k, Tuple) else k
                    v = TupleHash(v) if isinstance(v, Tuple) else v
                    newItems.append((k, v))
                val = tuple(newItems)

            elif isinstance(val, (list, set)):
                newItems = [
                    TupleHash(v)._key() if isinstance(v, Tuple) else v
                    for v in val
                ]
                val = tuple(newItems)

            elif isinstance(val, Tuple):
                val = TupleHash(val)._key()
                # val = TupleHash(val)

            vals.append((name, val))
        return tuple(vals)

    def __eq__(self, other):
        if not isinstance(other, TupleHash):
            return False
        return self._key() == other._key()

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        # This issue is caused by uiData and it's deep structure
        key = self._key()
        try:
            return hash(key)
        except Exception as e:
            raise e

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
from copy import deepcopy
from datetime import datetime
from typing import List, Dict, Set

from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.properties import RelationshipProperty
from sqlalchemy.sql.schema import Sequence

from .Jsonable import Jsonable
from .SerialiseUtil import T_RAPUI_TUPLE
from .SerialiseUtil import convertFromWkbElement, ISO8601, WKBElement

TUPLE_TYPES = []
TUPLE_TYPES_BY_NAME: Dict[str, object] = {}
_TUPLE_SHORT_NAMES: Set[str] = set()

JSON_EXCLUDE = "jsonExclude"


def registeredTupleNames():
    return list(TUPLE_TYPES_BY_NAME.keys())


def tupleForTupleName(tupleName):
    return TUPLE_TYPES_BY_NAME[tupleName]


def addTupleType(cls):
    tupleType = cls.tupleName()
    tupleTypeShort = cls.__tupleTypeShort__
    if tupleType in TUPLE_TYPES_BY_NAME or tupleType is None:
        raise Exception("Tuple name is None or is already in registered.\n"
                        "Tuple name is %s" % tupleType)

    if tupleTypeShort:
        if tupleTypeShort in _TUPLE_SHORT_NAMES:
            raise Exception("Tuple short name is already registered.\n"
                            "Tuple short name is %s" % tupleTypeShort)

        _TUPLE_SHORT_NAMES.add(tupleTypeShort)

    # Setup the lookups
    TUPLE_TYPES.append(cls)
    TUPLE_TYPES_BY_NAME[tupleType] = cls

    def underscoreException():
        raise Exception('TupleFields can not start with an underscore. '
                        'They can potentially clash with inner tuple workings'
                        ' if they do.')

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

        if name.startswith('_'):
            underscoreException()

        if shortName.startswith('_'):
            underscoreException()

        value.name = name
        fields.append(name)

        if shortName != JSON_EXCLUDE:
            if shortName in shortFieldNames:
                raise Exception('TupleField %s short name %s is alread registered'
                                % (name, shortName))

            shortFieldNames.add(shortName)

    hasSlots = hasattr(cls, "__slots__")
    hasFieldNames = cls.__fieldNames__ is not None

    if hasSlots and hasFieldNames:
        raise Exception("Only one of __slots__ or __fieldNames__ can be defined"
                        " but not both")

    # If field names already exist, then work off these
    if hasFieldNames:
        fields = list(set(fields + list(cls.__fieldNames__)))

        # Just check that the field names are defined.
        for fieldName in fields:
            if not hasattr(cls, fieldName):
                raise Exception("Tuple %s doesn't have field %s" % (tupleType, fieldName))

    # If field names already exist, then work off these
    if hasSlots:
        fields = list(set(fields + list(cls.__slots__)))

    # Sort and set the field names
    fields.sort()
    cls.__fieldNames__ = fields

    return cls


def removeTuplesForPackage(packageName):
    tupleNames = [cls.tupleName()
                  for cls in TUPLE_TYPES
                  if cls.__name__.startswith("%s." % packageName)]

    removeTuplesForTupleNames(tupleNames)


def removeTuplesForTupleNames(tupleNames):
    global TUPLE_TYPES, TUPLE_TYPES_BY_NAME, _TUPLE_SHORT_NAMES

    tupleNames = set(tupleNames)

    def filt(cls):
        return cls.tupleName() not in tupleNames

    tupleShortNames = [cls.__tupleTypeShort__
                       for cls in TUPLE_TYPES
                       if filt(cls)]

    # Remove from tuple types
    TUPLE_TYPES = list(filter(filt, TUPLE_TYPES))

    # Remove from tuple types by name
    for tupleName in tupleNames:
        if tupleName in TUPLE_TYPES_BY_NAME:
            del TUPLE_TYPES_BY_NAME[tupleName]

    # Remove from tuple short names
    _TUPLE_SHORT_NAMES = _TUPLE_SHORT_NAMES - set(tupleShortNames)


class TupleField(object):
    class _Map():
        pass

    def __init__(self, defaultValue=None, typingType=None, comment="", shortName=None,
                 jsonExclude=False):
        self.name = None
        self.shortName = shortName
        self.defaultValue = defaultValue
        self.typingType = typingType
        self.comment = comment
        self.jsonExclude = jsonExclude


class Tuple(Jsonable):
    ''' Tuple Type, EG com.synerty.rapui.UnitTestTuple'''
    __tupleType__ :str= None
    __tupleTypeShort__ = None
    __fieldNames__ = None
    __rapuiSerialiseType__ = T_RAPUI_TUPLE

    def __init__(self, **kwargs):
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
                raise KeyError("kwargs %s were passed, but tuple %s has no such fields"
                               % (', '.join(kwargs), self.__tupleType__))

            return

        # Reset all the tuples.
        # We never want TupleField in an instance
        for name in self.__fieldNames__:
            tupleField = getattr(self.__class__, name)

            if isinstance(tupleField, TupleField):
                setattr(self, name, deepcopy(tupleField.defaultValue))

            elif isinstance(tupleField, InstrumentedAttribute):
                default = (self.__table__.c[name].default
                           if name in self.__table__.c else
                           None)

                assign = (default is not None
                          and getattr(self, name) is None
                          and not isinstance(default, Sequence))
                if assign:
                    setattr(self, name, deepcopy(default.arg))

        # It's faster to add these at the end, rather than have logic to add one or
        # the other
        for key, val in kwargs.items():
            if not hasattr(self, key):
                raise KeyError("kwarg %s was passed, but tuple %s has no such TupleField"
                               % (key, self.__tupleType__))
            setattr(self, key, val)

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
        if not hasattr(other, '__tupleType__'):
            return False
        return cls.__tupleType__ == other.__tupleType__

    def tupleClone(self):
        return self.__class__(**{name: getattr(self, name)
                                 for name in self.__fieldNames__
                                 if hasattr(self, name)})

    def tupleToSmallJsonDict(self):
        json = {'_tt': (self.__tupleTypeShort__
                        if self.__tupleTypeShort__ else
                        self.__tupleType__)}

        for field in self.__class__.__dict__.values():

            if isinstance(field, TupleField):
                if field.jsonExclude:
                    continue

                key = field.shortName if field.shortName else field.name

            elif isinstance(field, InstrumentedAttribute):
                if isinstance(field.comparator.prop, RelationshipProperty):
                    continue

                key = field.doc if field.doc else field.name

            else:
                continue

            # Underscore means skip
            if key == JSON_EXCLUDE:
                continue

            def convert(value):
                if isinstance(value, list):
                    return [convert(v) for v in value]

                elif isinstance(value, Tuple):
                    return value.tupleToSmallJsonDict()

                elif isinstance(value, WKBElement):
                    return convertFromWkbElement(value)

                elif isinstance(value, TupleField):
                    return None

                elif isinstance(value, datetime):
                    return value.strftime(ISO8601)

                else:
                    return value

            json[key] = convert(getattr(self, field.name))

        return json

    def tupleToSqlaBulkInsertDict(self):
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

            insertDict[field.name] = convert(getattr(self, field.name))

        return insertDict

    def _fromJson(self, jsonStr: str):
        jsonDict = json.loads(jsonStr)

        assert (jsonDict[Jsonable.JSON_CLASS_TYPE] == self.__rapuiSerialiseType__)
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
        val = lambda name: (getattr(self, name)
                            if hasattr(self, name) else
                            'AttributeError')

        vals = ['type = %s,' % self.tupleType()]
        vals.extend(['%s = %s,' % (name, val(name)) for name in self.__fieldNames__])

        return '\n'.join(vals)


class TupleHash(object):
    def __init__(self, tupl):
        self.tupl = tupl
        from .Payload import Payload
        assert (tupl is not None)
        assert (isinstance(tupl, Tuple) or isinstance(tupl, Payload))

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
                newItems = [TupleHash(v)._key() if isinstance(v, Tuple) else v
                            for v in val]
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

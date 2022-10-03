"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : https://www.synerty.com
 * Support : support@synerty.com
"""
import logging
from typing import Type, Dict, Union

from twisted.internet.defer import Deferred, inlineCallbacks

from vortex.DeferUtil import callMethodLater
from vortex.DataLoaderDelegate import TupleDataLoaderDelegateABC
from vortex.Payload import Payload
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.Tuple import TupleField, addTupleType, Tuple
from vortex.TupleAction import TupleActionABC
from vortex.TupleActionVortex import TupleActionVortex
from vortex.TupleSelector import TupleSelector
from vortex.handler.TupleActionProcessor import (
    TupleActionProcessorDelegateABC,
    TupleActionProcessor,
)
from vortex.handler.TupleDataObservableHandler import (
    TupleDataObservableHandler,
    TuplesProviderABC,
)

logger = logging.getLogger(__name__)


class _DataLoaderDelegateState:
    def __init__(self, delegate):
        self.delegate = delegate
        # TODO: Make this a vortexMsgByTs instead of liveValueByTs
        self.liveValueByTs = {}


STORE_ACTION = "store"
EDIT_UPDATE_ACTION = "edit_update"
LOAD_ACTION = "load"
DELETE_ACTION = "delete"


@addTupleType
class _DataLoaderTupleAction(TupleActionABC):
    __tupleType__ = "vortex._DataLoaderTupleAction"

    data: Type[Tuple] = TupleField()
    action: str = TupleField()
    selector: TupleSelector = TupleField()


@addTupleType
class _LockDataTupleAction(TupleActionABC):
    __tupleType__ = "vortex._LockDataTupleAction"

    selector: TupleSelector = TupleField()
    lock: bool = TupleField()


@addTupleType
class _DataLockStatusTuple(Tuple):
    __tupleType__ = "vortex._DataLockStatusTuple"

    locked: bool = TupleField()


class TupleDataLoader(TupleActionProcessorDelegateABC, TuplesProviderABC):
    """Tuple Data Loader

    The `TupleDataLoader class is responsible for maintaining the
    `TupleDataObservableHandler` and `TupleActionProcessor` pair for each module
    which uses TupleForms. Forms (`Tuple`) and their corresponding
    delegate (`TupleDataLoaderDelegateABC`) are registered with the modules form loader
    using the `setDelegate` method. `TupleDataLoader`s lifecycle should be
    managed with the `shutdown` method in the UI controllers
    `shutdown` method. The `TupleDataLoader` can (should??) be initialized in
    the `start` method of the UI controller.

    Each instance of the `TupleDataLoader` registers itself as the delegate for
    every form (which itself is a `TupleActionABC`) registered with it. The
    `TupleDataLoaderDelegateABC` methods are used for handling reads and writes.

    TODO: Locking

    """

    def __init__(
        self,
        observable: TupleDataObservableHandler,
        actionProcessor: TupleActionProcessor,
    ):
        self._observable = observable
        self._actionProcessor = actionProcessor

        self._registeredDataDelegateByDataType: Dict[
            str, _DataLoaderDelegateState
        ] = {}

        self._lockingUuidByTs: Dict[TupleSelector, str] = {}

    def start(self):
        """Start

        Start all registered handlers

        """
        self._observable.addTupleProvider(
            _DataLockStatusTuple.tupleType(), self
        )

        self._actionProcessor.setDelegate(
            _DataLoaderTupleAction.tupleType(),
            self,
        )
        self._actionProcessor.setDelegate(
            _LockDataTupleAction.tupleType(), self
        )

        for entry in self._registeredDataDelegateByDataType.values():
            entry.delegate.start()

    def shutdown(self) -> None:
        """Shutdown

        Shutdown the observable and action processor managed with this
        TupleDataLoader and the registered handlers

        :return: None
        """
        for entry in self._registeredDataDelegateByDataType.values():
            entry.delegate.shutdown()

        self._observable.shutdown()
        self._actionProcessor.shutdown()

    def setDelegate(
        self,
        DataClass: Union[Type[Tuple], str],
        delegate: TupleDataLoaderDelegateABC,
    ) -> None:
        """Set Delegate

        Sets `self` as the delegate for `DataClass` which is a TupleActionABC
        and registers `handler` as the form data handler for `FormClass` which
        is used for reading and writing the data

        :param DataClass: `Tuple` to register the handler for
        :param delegate: Instance of `TupleDataLoaderDelegateABC` that provides R/W
        :return: None
        """
        tupleType = (
            DataClass if isinstance(DataClass, str) else DataClass.tupleType()
        )

        self._observable.addTupleProvider(tupleType, self)
        self._registeredDataDelegateByDataType[
            tupleType
        ] = _DataLoaderDelegateState(delegate=delegate)

    @inlineCallbacks
    def makeVortexMsg(
        self, filt: dict, tupleSelector: TupleSelector
    ) -> Union[Deferred, bytes]:
        if tupleSelector.name == _DataLockStatusTuple.tupleType():
            selector = TupleSelector().fromJsonDict(tupleSelector.selector)

            result = _DataLockStatusTuple()
            result.locked = False

            if selector in self._lockingUuidByTs:
                result.locked = True
        else:
            tupleType = tupleSelector.name

            if tupleType not in self._registeredDataDelegateByDataType:
                raise ValueError(
                    f"Delegate for {tupleType} not registered with "
                    f"{self.__class__}"
                )

            entry = self._registeredDataDelegateByDataType[tupleType]
            if entry.liveValueByTs.get(tupleSelector) is None:
                result = yield entry.delegate.loadData(tupleSelector)
            else:
                result = entry.liveValueByTs.get(tupleSelector)

        results = [result]

        payloadEnvelope = PayloadEnvelope(filt=filt)
        payloadEnvelope.encodedPayload = Payload(
            filt=filt, tuples=results
        ).toEncodedPayload()
        vortexMsg = payloadEnvelope.toVortexMsg()

        return vortexMsg

    @inlineCallbacks
    def processTupleAction(
        self,
        tupleAction: _DataLoaderTupleAction,
        tupleActionVortex: TupleActionVortex,
    ) -> Deferred:
        lockingTs = TupleSelector(
            name=_DataLockStatusTuple.tupleType(),
            selector={
                "name": tupleAction.selector.name,
                "selector": tupleAction.selector.selector,
            },
        )

        if isinstance(tupleAction, _LockDataTupleAction):
            if tupleAction.lock:
                if tupleAction.selector in self._lockingUuidByTs:
                    raise Exception("Data already locked")

                self._lockingUuidByTs[
                    tupleAction.selector
                ] = tupleActionVortex.uuid
            else:
                if tupleAction.selector not in self._lockingUuidByTs:
                    raise Exception("Data not locked")

                lockingUuid = self._lockingUuidByTs[tupleAction.selector]

                if lockingUuid != tupleActionVortex.uuid:
                    raise Exception("Only the locker can unlock the data")

                self._lockingUuidByTs.pop(tupleAction.selector)
            self.__notifyOfTupleUpdate(lockingTs)
            return

        if (
            tupleAction.data.tupleType()
            not in self._registeredDataDelegateByDataType
        ):
            raise ValueError(
                f"Delegate for {tupleAction.data.tupleType()} not registered with "
                f"{self.__class__}"
            )

        entry = self._registeredDataDelegateByDataType[
            tupleAction.data.tupleType()
        ]

        if tupleAction.selector in self._lockingUuidByTs:
            if (
                tupleActionVortex.uuid
                != self._lockingUuidByTs[tupleAction.selector]
            ):
                raise Exception("Data is locked")

        data = None
        if tupleAction.action == STORE_ACTION:
            data = yield entry.delegate.storeData(
                tupleAction.data, tupleAction.selector.selector
            )
            entry.liveValueByTs[tupleAction.selector] = None

            # Unlock the form if it is locked
            if tupleAction.selector in self._lockingUuidByTs:
                self._lockingUuidByTs.pop(tupleAction.selector)
                self.__notifyOfTupleUpdate(lockingTs)

        elif tupleAction.action == LOAD_ACTION:
            # It looks like nothing is done for LOAD_ACTION but the work is done
            # in the makeVortexMsg method
            entry.liveValueByTs[tupleAction.selector] = None

            # Unlock the form if it is locked
            if tupleAction.selector in self._lockingUuidByTs:
                self._lockingUuidByTs.pop(tupleAction.selector)
                self.__notifyOfTupleUpdate(lockingTs)

        elif tupleAction.action == DELETE_ACTION:
            yield entry.delegate.deleteData(tupleAction.selector.selector)
            entry.liveValueByTs[tupleAction.selector] = None

            # Unlock the form if it is locked
            if tupleAction.selector in self._lockingUuidByTs:
                self._lockingUuidByTs.pop(tupleAction.selector)
                self.__notifyOfTupleUpdate(lockingTs)

        elif tupleAction.action == EDIT_UPDATE_ACTION:
            entry.liveValueByTs[tupleAction.selector] = tupleAction.data

        self.__notifyOfTupleUpdate(tupleAction.selector)

        return data

    @callMethodLater
    def __notifyOfTupleUpdate(self, ts: TupleSelector):
        self._observable.notifyOfTupleUpdate(ts)

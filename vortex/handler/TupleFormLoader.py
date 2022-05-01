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
from vortex.FormLoader import FormLoaderTupleABC, TupleFormLoaderDelegateABC
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


class _FormLoaderDelegateState:
    def __init__(self, delegate):
        self.delegate = delegate
        # TODO: Make this a vortexMsgByTs instead of liveValueByTs
        self.liveValueByTs = {}


SAVE_ACTION = "save"
LIVE_UPDATE_ACTION = "live_update"
REVERT_ACTION = "revert"


@addTupleType
class _FormLoaderTupleAction(TupleActionABC):
    __tupleType__ = "vortex._FormLoaderTupleAction"

    data: FormLoaderTupleABC = TupleField()
    action: str = TupleField()
    selector: TupleSelector = TupleField()


@addTupleType
class _LockFormTupleAction(TupleActionABC):
    __tupleType__ = "vortex._LockFormTupleAction"

    selector: TupleSelector = TupleField()
    lock: bool = TupleField()


@addTupleType
class _FormLockStatusTuple(Tuple):
    __tupleType__ = "vortex._FormLockStatusTuple"

    locked: bool = TupleField()


class TupleFormLoader(TupleActionProcessorDelegateABC, TuplesProviderABC):
    """Tuple Form Loader

    The `TupleFormLoader class is responsible for maintaining the
    `TupleDataObservableHandler` and `TupleActionProcessor` pair for each module
    which uses TupleForms. Forms (`FormLoaderTupleABC`) and their corresponding
    delegate (`TupleFormLoaderDelegateABC`) are registered with the modules form loader
    using the `setDelegate` method. `TupleFormLoader`s lifecycle should be
    managed with the `shutdown` method in the UI controllers
    `shutdown` method. The `TupleFormLoader` can (should??) be initialized in
    the `start` method of the UI controller.

    Each instance of the `TupleFormLoader` registers itself as the delegate for
    every form (which itself is a `TupleActionABC`) registered with it. The
    `TupleFormLoaderDelegateABC` methods are used for handling reads and writes.

    TODO: Locking

    """

    def __init__(
        self,
        observable: TupleDataObservableHandler,
        actionProcessor: TupleActionProcessor,
    ):
        self._observable = observable
        self._actionProcessor = actionProcessor

        self._registeredFormDelegateByFormType: Dict[
            str, _FormLoaderDelegateState
        ] = {}

        self._lockingUuidByTs: Dict[TupleSelector, str] = {}

    def start(self):
        """Start

        Start all registered handlers

        """
        self._observable.addTupleProvider(
            _FormLockStatusTuple.tupleType(), self
        )

        self._actionProcessor.setDelegate(
            _FormLoaderTupleAction.tupleType(),
            self,
        )
        self._actionProcessor.setDelegate(
            _LockFormTupleAction.tupleType(), self
        )

        for entry in self._registeredFormDelegateByFormType.values():
            entry.delegate.start()

    def shutdown(self) -> None:
        """Shutdown

        Shutdown the observable and action processor managed with this
        TupleFormLoader and the registered handlers

        :return: None
        """
        for entry in self._registeredFormDelegateByFormType.values():
            entry.delegate.shutdown()

        self._observable.shutdown()
        self._actionProcessor.shutdown()

    def setDelegate(
        self,
        FormClass: Union[Type[FormLoaderTupleABC], str],
        delegate: TupleFormLoaderDelegateABC,
    ) -> None:
        """Set Delegate

        Sets `self` as the delegate for `FormClass` which is a TupleActionABC
        and registers `handler` as the form data handler for `FormClass` which
        is used for reading and writing the data

        :param FormClass: `FormLoaderTupleABC` to register the handler for
        :param delegate: Instance of `TupleFormLoaderDelegateABC` that provides R/W
        :return: None
        """
        tupleType = (
            FormClass if isinstance(FormClass, str) else FormClass.tupleType()
        )

        self._observable.addTupleProvider(tupleType, self)
        self._registeredFormDelegateByFormType[
            tupleType
        ] = _FormLoaderDelegateState(delegate=delegate)

    @inlineCallbacks
    def makeVortexMsg(
        self, filt: dict, tupleSelector: TupleSelector
    ) -> Union[Deferred, bytes]:
        if tupleSelector.name == _FormLockStatusTuple.tupleType():
            selector = TupleSelector().fromJsonDict(tupleSelector.selector)

            result = _FormLockStatusTuple()
            result.locked = False

            if selector in self._lockingUuidByTs:
                result.locked = True
        else:
            tupleType = tupleSelector.name

            if tupleType not in self._registeredFormDelegateByFormType:
                raise ValueError(
                    f"Delegate for {tupleType} not registered with "
                    f"{self.__class__}"
                )

            entry = self._registeredFormDelegateByFormType[tupleType]
            if entry.liveValueByTs.get(tupleSelector) is None:
                result = yield entry.delegate.readData(tupleSelector.selector)
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
        tupleAction: _FormLoaderTupleAction,
        tupleActionVortex: TupleActionVortex,
    ) -> Deferred:
        lockingTs = TupleSelector(
            name=_FormLockStatusTuple.tupleType(),
            selector={
                "name": tupleAction.selector.name,
                "selector": tupleAction.selector.selector,
            },
        )

        if isinstance(tupleAction, _LockFormTupleAction):
            if tupleAction.lock:
                if tupleAction.selector in self._lockingUuidByTs:
                    raise Exception("Form already locked")

                self._lockingUuidByTs[
                    tupleAction.selector
                ] = tupleActionVortex.uuid
            else:
                if tupleAction.selector not in self._lockingUuidByTs:
                    raise Exception("Form not locked")

                lockingUuid = self._lockingUuidByTs[tupleAction.selector]

                if lockingUuid != tupleActionVortex.uuid:
                    raise Exception("Only the locker can unlock the form")

                self._lockingUuidByTs.pop(tupleAction.selector)
            self.__notifyOfTupleUpdate(lockingTs)
            return

        if (
            tupleAction.data.tupleType()
            not in self._registeredFormDelegateByFormType
        ):
            raise ValueError(
                f"Delegate for {tupleAction.data.tupleType()} not registered with "
                f"{self.__class__}"
            )

        entry = self._registeredFormDelegateByFormType[
            tupleAction.data.tupleType()
        ]

        if tupleAction.selector in self._lockingUuidByTs:
            if (
                tupleActionVortex.uuid
                != self._lockingUuidByTs[tupleAction.selector]
            ):
                raise Exception("Form is locked")

        if tupleAction.action == SAVE_ACTION:
            yield entry.delegate.writeData(
                tupleAction.data, tupleAction.selector.selector
            )
            entry.liveValueByTs[tupleAction.selector] = None

            # Unlock the form if it is locked
            if tupleAction.selector in self._lockingUuidByTs:
                self._lockingUuidByTs.pop(tupleAction.selector)
                self.__notifyOfTupleUpdate(lockingTs)

        elif tupleAction.action == REVERT_ACTION:
            entry.liveValueByTs[tupleAction.selector] = None

            # Unlock the form if it is locked
            if tupleAction.selector in self._lockingUuidByTs:
                self._lockingUuidByTs.pop(tupleAction.selector)
                self.__notifyOfTupleUpdate(lockingTs)

        elif tupleAction.action == LIVE_UPDATE_ACTION:
            entry.liveValueByTs[tupleAction.selector] = tupleAction.data

        self.__notifyOfTupleUpdate(tupleAction.selector)

    @callMethodLater
    def __notifyOfTupleUpdate(self, ts: TupleSelector):
        self._observable.notifyOfTupleUpdate(ts)

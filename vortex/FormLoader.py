"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : https://www.synerty.com
 * Support : support@synerty.com
"""
import abc
from abc import abstractmethod
from typing import Union, Dict, Any

from twisted.internet.defer import Deferred, inlineCallbacks

from vortex.Tuple import Tuple
from vortex.TupleSelector import TupleSelector


# TODO: Get rid of this, *maybe*?
class FormLoaderTupleABC(Tuple):
    """Form Loader Tuple ABC

    This class marks the inheriting class as a Tuple to be used as data with
    the `TupleFormLoader`
    """

    pass


class TupleFormLoaderDelegateABC(abc.ABC):
    """Tuple Form Loader Delegate ABC

    ABC for delegate classes to handle the reading and writing for each forms'
    data. Handlers must be register in
    (`FormLoaderTupleABC`, `TupleFormLoaderDelegateABC`) pairs with the
    `TupleFormLoader`

    """

    def start(self):
        pass

    def shutdown(self):
        pass

    @abstractmethod
    @inlineCallbacks
    def readData(
            self, selector: Dict[str, Any]
    ) -> Union[Deferred, FormLoaderTupleABC]:
        pass

    @abstractmethod
    @inlineCallbacks
    def writeData(self, data: FormLoaderTupleABC,
                  selector: TupleSelector) -> Deferred:
        pass

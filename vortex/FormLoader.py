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


class TupleDataLoaderDelegateABC(abc.ABC):
    """Tuple Data Loader Delegate ABC

    ABC for delegate classes to handle the reading and writing for each forms'
    data. Handlers must be register in
    (`Tuple`, `TupleDataLoaderDelegateABC`) pairs with the `TupleDataLoader`

    """

    def start(self):
        pass

    def shutdown(self):
        pass

    @abstractmethod
    @inlineCallbacks
    def loadData(self, selector: TupleSelector) -> Union[Deferred, Tuple]:
        pass

    @abstractmethod
    @inlineCallbacks
    def storeData(self, data: Tuple, selector: TupleSelector) -> Deferred:
        pass

    @abstractmethod
    @inlineCallbacks
    def deleteData(self, selector: TupleSelector) -> Deferred:
        pass

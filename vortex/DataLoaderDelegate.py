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
    def loadData(self, selector: TupleSelector) -> Union[Deferred, Tuple]:
        """Load Data

        The `loadData` method is called when data needs to be loaded from a
        storage medium. The query is represented by a `TupleSelector` value
        `selector`.

        :param selector: The `TupleSelector` for which to load data
        :return: The `Tuple` or a `Deferred` which eventually results in a `Tuple`
        """
        pass

    @abstractmethod
    def storeData(self, data: Tuple, selector: TupleSelector) -> Deferred:
        """Store Data

        Store the data into the storage medium. The `data` is the `Tuple` which
        needs to be written

        :param data: Tuple to write and store
        :param selector: Previous selector used
        :return: Deferred
        """
        pass

    @abstractmethod
    def deleteData(self, selector: TupleSelector) -> Deferred:
        """Delete Data

        Delete the data referenced by the `selector`

        :param selector: `TupleSelector` for data to be deleted
        :return: Deferred
        """
        pass

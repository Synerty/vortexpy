from abc import ABCMeta, abstractmethod
from typing import Optional, Union, Callable, List

from twisted.internet.defer import Deferred

from vortex.PayloadEnvelope import VortexMsgList

SendVortexMsgResponseCallable = Callable[
    [Union[VortexMsgList, bytes], int], Deferred
]


class VortexInfo:
    """Vortex Info

    This class stores details about the end of a vortex.
    """

    def __init__(self, name, uuid):
        self.name, self.uuid = name, uuid

    def __repr__(self):
        return "VortexInfo, Name=%s, UUID=%s" % (self.name, self.uuid)


class VortexABC(metaclass=ABCMeta):
    @property
    @abstractmethod
    def localVortexInfo(self) -> VortexInfo:
        pass

    @property
    @abstractmethod
    def remoteVortexInfo(self) -> List[VortexInfo]:
        pass

    @abstractmethod
    def sendVortexMsg(
        self,
        vortexMsgs: Union[VortexMsgList, bytes],
        vortexUuid: Optional[str] = None,
    ):
        """Send Vortex Msg

        Sends the vortex message to any conencted clients with vortexUuid.
        Or broadcast it to all connected vortex clients if it's None

        :param vortexMsgs: The vortex message(s) to send
        :param vortexUuid: The vortexUuid of the client to send to.
        """

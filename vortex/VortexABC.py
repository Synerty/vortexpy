from abc import ABCMeta, abstractproperty, abstractmethod
from typing import Optional


class VortexInfo:
    """ Vortex Info

    This class stores details about the end of a vortex.
    """
    def __init__(self, name, uuid):
        self.name, self.uuid = name, uuid


class VortexABC(metaclass=ABCMeta):


    @abstractproperty
    def localVortexInfo(self) -> VortexInfo:
        pass

    @abstractproperty
    def remoteVortexInfo(self) -> [VortexInfo]:
        pass

    @abstractmethod
    def sendVortexMsg(self, vortexMsg: bytes, vortexUuid: Optional[str] = None):
        """ Send Vortex Msg

        Sends the vortex message to any conencted clients with vortexUuid.
        Or broadcast it to all connected vortex clients if it's None

        :param vortexMsg: The vortex message to send
        :param vortexUuid: The vortexUuid of the client to send to.
        """

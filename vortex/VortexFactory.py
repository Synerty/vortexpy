from typing import Union, List, Optional

from twisted.internet.defer import Deferred

from vortex.Payload import VortexMsgList
from vortex.VortexABC import VortexABC
from vortex.VortexClient import VortexClient
from vortex.VortexResource import VortexResource
from vortex.VortexServer import VortexServer

broadcast = None


class NoVortexException(Exception):
    """ No Vortex Exception

    This is raised when a remote vortex doesn't exist.

    """


VortexUuidList = List[str]


class VortexFactory:
    __vortexServersByName = {}
    __vortexClientsByName = {}
    __remoteVortexexByLocalVortex = {}

    def __init__(self):
        raise Exception("Vortex Factory should not be instantiated")

    @classmethod
    def _getVortexSendRefs(cls, name) -> (VortexABC, [str]):
        results = []

        vortexes = (list(cls.__vortexServersByName.values())
                    + list(cls.__vortexServersByName.values()))

        for vortex in vortexes:
            uuids = []
            for remoteVortexInfo in vortex.remoteVortexInfo:
                if remoteVortexInfo.name == name:
                    uuids.append(remoteVortexInfo.uuid)

            if uuids:
                results.append((vortex, uuids))

        return results

    @classmethod
    def createServer(cls, name: str, rootResource) -> None:
        """ Create Server

        Create a vortex server, VortexServer clients connect to this vortex serer via HTTP(S)

        VortexServer clients will connect and provide their names. This allows the factory to
        abstract away the vortex UUIDs from their names.

        :param name: The name of the local vortex.
        :param rootResource: The resource to add the vortex to. An implementation of
            C{twisted.web.resource.IResource}
        :return: None
        """

        vortexServer = VortexServer(name)
        vortexResource = VortexResource(vortexServer)
        rootResource.putChild(b"vortex", vortexResource)

    @classmethod
    def createClient(cls, name: str, host: str, port: int):
        """ Create Client

        Connect to a vortex Server.

        :param name: The name of the local vortex.
        :param host: The hostname of the remote vortex.
        :param port: The port of the remote vortex.

        """

        vortexClient = VortexClient(name)
        return vortexClient.connect(host, port)

    @classmethod
    def vortexClient(cls, name: str) -> VortexClient:
        if not name in cls.__vortexClientsByName:
            raise NoVortexException("VortexClient %s doesn't exist" % name)

        return cls.__vortexClientsByName[name]

    @classmethod
    def sendVortexMsg(cls,
                      vortexMsgs: Union[VortexMsgList, bytes],
                      destVortexName: Optional[str] = broadcast) -> Deferred:
        """ Send VortexMsg

        Sends a payload to the remote vortex, by name.

        :param destVortexName: The name of the vortex to send the payload to.
        :param vortexMsgs: The vortex message(s) to send to the remote vortex.
        :return: A C{Deferred} which will callback when the message has been sent.
        """

        vortexAndUuids = cls._getVortexSendRefs(destVortexName)
        if not vortexAndUuids:
            raise NoVortexException("Can not find vortexes named %s to send message to"
                                     % destVortexName)

        for vortex, uuids in vortexAndUuids:
            for uuid in uuids:
                vortex.sendVortexMsg(vortexMsgs, uuid)

    # def vortexIsClientAlive(vortexUuid):
    #     return VortexServer().isVortexAlive(vortexUuid)
    #
    #
    # def vortexClientIpPort(vortexUuid):
    #     return VortexServer().vortexClientIpPort(vortexUuid)

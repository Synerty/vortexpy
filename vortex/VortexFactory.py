import logging
from collections import defaultdict
from typing import Union, List, Optional

from twisted.internet.defer import Deferred, DeferredList

from vortex.Payload import VortexMsgList
from vortex.VortexABC import VortexABC
from vortex.VortexClient import VortexClient
from vortex.VortexResource import VortexResource
from vortex.VortexServer import VortexServer

logger = logging.getLogger(__name__)

broadcast = None


class NoVortexException(Exception):
    """ No Vortex Exception

    This is raised when a remote vortex doesn't exist.

    """


VortexUuidList = List[str]


class VortexFactory:
    __vortexServersByName = defaultdict(list)
    __vortexClientsByName = defaultdict(list)
    __remoteVortexexByLocalVortex = {}

    def __init__(self):
        raise Exception("Vortex Factory should not be instantiated")

    @classmethod
    def _getVortexSendRefs(cls, name=None, uuid=None) -> (VortexABC, [str]):
        assert name or uuid

        results = []

        vortexes = []

        for vortexList in cls.__vortexServersByName.values():
            vortexes += vortexList

        for vortexList in cls.__vortexClientsByName.values():
            vortexes += vortexList

        # logger.debug("-" * 80)
        for vortex in vortexes:
            uuids = []
            # logger.debug("FROM : %s", vortex.localVortexInfo)
            for remoteVortexInfo in vortex.remoteVortexInfo:
                # logger.debug("        REMOTE : %s", remoteVortexInfo)
                if ((name is None or remoteVortexInfo.name == name)
                    and (uuid is None or remoteVortexInfo.uuid == uuid)):
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
        cls.__vortexServersByName[name].append(vortexServer)

        vortexResource = VortexResource(vortexServer)
        rootResource.putChild(b"vortex", vortexResource)

    @classmethod
    def createClient(cls, name: str, host: str, port: int) -> Deferred:
        """ Create Client

        Connect to a vortex Server.

        :param name: The name of the local vortex.
        :param host: The hostname of the remote vortex.
        :param port: The port of the remote vortex.

        :return: A deferred from the VortexClient.connect method
        """

        logger.info('Connecting to Peek Server %s:%s', host, port)

        vortexClient = VortexClient(name)
        cls.__vortexClientsByName[name].append(vortexClient)

        return vortexClient.connect(host, port)

    @classmethod
    def getLocalVortexClients(cls, localVortexName: str) -> List[VortexClient]:
        return list(filter(lambda x: x.name == localVortexName,
                           cls.__vortexClientsByName.values()))

    @classmethod
    def sendVortexMsg(cls,
                      vortexMsgs: Union[VortexMsgList, bytes],
                      destVortexName: Optional[str] = broadcast,
                      destVortexUuid: Optional[str] = broadcast) -> Deferred:
        """ Send VortexMsg

        Sends a payload to the remote vortex.

        :param vortexMsgs: The vortex message(s) to send to the remote vortex.

        :param destVortexName: The name of the vortex to send the payload to.
            This can be null, If provided, it's used to limit the vortexes the
             message is sent to.

        :param destVortexUuid: The uuid of the vortex to send the payload to,
            This can be null, If provided, it's used to limit the vortexes the
             message is sent to.


        :return: A C{Deferred} which will callback when the message has been sent.
        """

        vortexAndUuids = cls._getVortexSendRefs(destVortexName, destVortexUuid)
        if not vortexAndUuids:
            raise NoVortexException("Can not find vortexes to send message to,"
                                    " name=%s, uuid=%s"
                                    % (destVortexName, destVortexUuid))

        deferreds = []
        for vortex, uuids in vortexAndUuids:
            for uuid in uuids:
                deferreds.append(vortex.sendVortexMsg(vortexMsgs, uuid))

        return DeferredList(deferreds)

        # def vortexIsClientAlive(vortexUuid):
        #     return VortexServer().isVortexAlive(vortexUuid)
        #
        #
        # def vortexClientIpPort(vortexUuid):
        #     return VortexServer().vortexClientIpPort(vortexUuid)

import logging
from collections import defaultdict

from rx.subjects import Subject
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, succeed
from twisted.python.failure import Failure
from txws import WebSocketFactory
from typing import Union, List, Optional

from vortex.Payload import VortexMsgList, Payload
from vortex.PayloadIO import PayloadIO
from vortex.VortexABC import VortexABC
from vortex.VortexClientHttp import VortexClientHttp
from vortex.VortexClientTcp import VortexClientTcp
from vortex.VortexServer import VortexServer
from vortex.VortexServerHttpResource import VortexServerHttpResource
from vortex.VortexServerTcp import VortexTcpServerFactory
from vortex.VortexServerWebsocket import VortexWebsocketServerFactory

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

    __vortexStatusChangeSubjectsByName = {}

    __isShutdown = False

    def __init__(self):
        raise Exception("Vortex Factory should not be instantiated")

    @classmethod
    def shutdown(cls) -> None:
        cls.__isShutdown = True

    @classmethod
    def _getVortexSendRefs(cls, name=None, uuid=None) -> (VortexABC, [str]):
        assert name or uuid

        results = []

        # logger.debug("-" * 80)
        for vortex in cls._allVortexes():
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
    def _allVortexes(cls):
        """ All Vortexes

        :return: A list of all the vortexes, both client and server
        """
        vortexes = []
        for vortexList in cls.__vortexServersByName.values():
            vortexes += vortexList
        for vortexList in cls.__vortexClientsByName.values():
            vortexes += vortexList

        return vortexes

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

        vortexResource = VortexServerHttpResource(vortexServer)
        rootResource.putChild(b"vortex", vortexResource)

    @classmethod
    def createWebsocketServer(cls, name: str, port: int) -> None:
        """ Create Server

        Create a vortex server, VortexServer clients connect to this vortex serer via HTTP(S)

        VortexServer clients will connect and provide their names. This allows the factory to
        abstract away the vortex UUIDs from their names.

        :param name: The name of the local vortex.
        :param port: The tcp port to listen on
        :return: None
        """

        vortexServer = VortexServer(name)
        cls.__vortexServersByName[name].append(vortexServer)
        vortexWebsocketServerFactory = VortexWebsocketServerFactory(vortexServer)
        reactor.listenTCP(port, WebSocketFactory(vortexWebsocketServerFactory))

    @classmethod
    def createTcpServer(cls, name: str, port: int) -> None:
        """ Create Server

        Create a vortex server, VortexServer clients connect to this vortex serer via HTTP(S)

        VortexServer clients will connect and provide their names. This allows the factory to
        abstract away the vortex UUIDs from their names.

        :param name: The name of the local vortex.
        :param port: The tcp port to listen on
        :return: None
        """

        vortexServer = VortexServer(name)
        cls.__vortexServersByName[name].append(vortexServer)
        vortexTcpServerFactory = VortexTcpServerFactory(vortexServer)
        reactor.listenTCP(port, vortexTcpServerFactory)

    @classmethod
    def createHttpClient(cls, name: str, host: str, port: int) -> Deferred:
        """ Create Client

        Connect to a vortex Server.

        :param name: The name of the local vortex.
        :param host: The hostname of the remote vortex.
        :param port: The port of the remote vortex.

        :return: A deferred from the VortexHttpClient.connect method
        """

        logger.info('Connecting to Peek Server HTTP %s:%s', host, port)

        vortexClient = VortexClientHttp(name)
        cls.__vortexClientsByName[name].append(vortexClient)

        return vortexClient.connect(host, port)

    @classmethod
    def createTcpClient(cls, name: str, host: str, port: int) -> Deferred:
        """ Create Client

        Connect to a vortex Server.

        :param name: The name of the local vortex.
        :param host: The hostname of the remote vortex.
        :param port: The port of the remote vortex.

        :return: A deferred from the VortexTcpClient.connect method
        """

        logger.info('Connecting to Peek Server TCP %s:%s', host, port)

        vortexClient = VortexClientTcp(name)
        cls.__vortexClientsByName[name].append(vortexClient)

        return vortexClient.connect(host, port)

    @classmethod
    def isVortexNameLocal(cls, vortexName: str) -> bool:
        for vortex in cls._allVortexes():
            if vortex.localVortexInfo.name == vortexName:
                return True

        return False

    @classmethod
    def getLocalVortexClients(cls, localVortexName: str) -> List[VortexABC]:
        return list(filter(lambda x: x.name == localVortexName,
                           cls.__vortexClientsByName.values()))

    @classmethod
    def getRemoteVortexUuids(cls) -> List[str]:
        remoteUuids = []

        for vortex in cls._allVortexes():
            for remoteVortexInfo in vortex.remoteVortexInfo:
                remoteUuids.append(remoteVortexInfo.uuid)

        return remoteUuids

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

    @classmethod
    def sendVortexMsgLocally(cls, vortexMsgs: Union[VortexMsgList, bytes]) -> Deferred:
        """ Send VortexMsg

        Sends a payload to the remote vortex.

        :param vortexMsgs: The vortex message(s) to deliver locally.

        :return: A C{Deferred} which will callback when the message has been delivered.
        """

        vortexUuid = "local"
        vortexName = "local"
        httpSession = "local"
        sendResponse = VortexFactory.sendVortexMsgLocally

        vortexMsgs = [vortexMsgs] if isinstance(vortexMsgs, bytes) else vortexMsgs

        def send(payload):
            try:
                PayloadIO().process(
                    payload=payload,
                    vortexUuid=vortexUuid,
                    vortexName=vortexName,
                    httpSession=httpSession,
                    sendResponse=sendResponse
                )
                return succeed(True)

            except Exception as e:
                return Failure(e)

        deferreds = []
        for vortexMsg in vortexMsgs:
            deferreds.append(send(Payload().fromVortexMsg(vortexMsg)))

        return DeferredList(deferreds)

    @classmethod
    def subscribeToVortexStatusChange(cls, vortexName:str) -> Subject:
        """ Subscribe to Vortex Status Change
        
        Subscribing to the returned observable/subject will provided updates of when the
        vortex goes offline, or online.
        
        .. warning:: This is only implemented for TCP Client vortexes.
        
        :param vortexName: The name of the vortex to subscribe to the status for.
                            This will be the name of the remote vortex.
        
        """
        if not vortexName in cls.__vortexStatusChangeSubjectsByName:
            cls.__vortexStatusChangeSubjectsByName[vortexName] = Subject()
        return cls.__vortexStatusChangeSubjectsByName[vortexName]

    @classmethod
    def _notifyOfVortexStatusChange(cls, vortexName:str, online:bool) -> None:
        if cls.__isShutdown:
            return

        logger.debug("Vortex %s went %s", vortexName, ("online" if online else "offline"))

        if vortexName in cls.__vortexStatusChangeSubjectsByName:
            cls.__vortexStatusChangeSubjectsByName[vortexName].on_next(online)

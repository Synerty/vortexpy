import logging
import typing
from collections import defaultdict
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from OpenSSL import SSL
from rx.subjects import Subject
from twisted.internet import reactor
from twisted.internet import ssl
from twisted.internet._sslverify import trustRootFromCertificates
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import succeed
from twisted.internet.task import deferLater
from twisted.python.failure import Failure

from txhttputil.util.PemUtil import parsePemBundleForClient
from txhttputil.util.PemUtil import parsePemBundleForServer
from txhttputil.util.PemUtil import parsePemBundleForTrustedPeers
from txhttputil.util.SslUtil import buildCertificateOptionsForTwisted
from txhttputil.util.SslUtil import parseTrustRootFromBundle
from txwebsocket.txws import WebSocketFactory
from vortex.DeferUtil import yesMainThread
from vortex.PayloadEnvelope import PayloadEnvelope
from vortex.PayloadEnvelope import VortexMsgList
from vortex.PayloadIO import PayloadIO
from vortex.PayloadPriority import DEFAULT_PRIORITY
from vortex.VortexABC import VortexABC
from vortex.VortexABC import VortexInfo
from vortex.VortexClientHttp import VortexClientHttp
from vortex.VortexClientTcp import VortexClientTcp
from vortex.VortexClientWebsocketFactory import VortexClientWebsocketFactory
from vortex.VortexServer import VortexServer
from vortex.VortexServerHttpResource import VortexServerHttpResource
from vortex.VortexServerTcp import VortexTcpServerFactory
from vortex.VortexServerWebsocket import VortexWebSocketUpgradeResource
from vortex.VortexServerWebsocket import VortexWebsocketServerFactory
from vortex.VortexServerWebsocket import VortexWrappedWebSocketFactory

logger = logging.getLogger(__name__)

broadcast = None

browserVortexName = "browser"


class NoVortexException(Exception):
    """No Vortex Exception

    This is raised when a remote vortex doesn't exist.

    """


VortexUuidList = List[str]


class VortexFactory:
    __vortexServersByName: Dict[str, List[VortexABC]] = defaultdict(list)
    __vortexClientsByName: Dict[str, List[VortexABC]] = defaultdict(list)

    __vortexStatusChangeSubjectsByName: Dict[str, Subject] = {}

    __isShutdown = False

    __listeningPorts = []

    def __init__(self):
        raise Exception("Vortex Factory should not be instantiated")

    @classmethod
    @inlineCallbacks
    def shutdown(cls):
        cls.__isShutdown = True
        for vortex in VortexFactory._allVortexes():
            if hasattr(vortex, "close"):
                vortex.close()
                yield deferLater(reactor, 0.05, lambda: None)
        while cls.__listeningPorts:
            cls.__listeningPorts.pop().stopListening()
            yield deferLater(reactor, 0.05, lambda: None)

    @classmethod
    def _getVortexSendRefs(
        cls, name=None, uuid=None
    ) -> List[typing.Tuple[VortexABC, List[str]]]:
        assert name or uuid

        results = []

        # logger.debug("-" * 80)
        for vortex in cls._allVortexes():
            uuids: List[str] = []
            # logger.debug("FROM : %s", vortex.localVortexInfo)
            for remoteVortexInfo in vortex.remoteVortexInfo:
                # logger.debug("        REMOTE : %s", remoteVortexInfo)
                if (name is None or remoteVortexInfo.name == name) and (
                    uuid is None or remoteVortexInfo.uuid == uuid
                ):
                    uuids.append(remoteVortexInfo.uuid)

            if uuids:
                results.append((vortex, uuids))

        return results

    @classmethod
    def _allVortexes(cls):
        """All Vortexes

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
        """Create Server

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
    def createWebsocketServer(
        cls,
        name: str,
        port: int,
        enableSsl: Optional[bool] = False,
        sslEnableMutualTLS: Optional[bool] = False,
        sslBundleFilePath: Optional[str] = None,
        sslMutualTLSCertificateAuthorityBundleFilePath: Optional[str] = None,
        sslMutualTLSTrustedPeerCertificateBundleFilePath: Optional[str] = None,
    ) -> None:
        """Create Server

        Create a vortex server, VortexServer clients connect to this vortex serer via HTTP(S)

        VortexServer clients will connect and provide their names. This allows the factory to
        abstract away the vortex UUIDs from their names.

        :param name: The name of the local vortex.
        :param port: The tcp port to listen on
        :param enableSsl: switch ssl on or off for HTTP
        :param sslEnableMutualTLS: switch on or off mTLS
        :param sslBundleFilePath: a filepath to a PEM file that contains
                            a private key, a certificate and a CA certificate
                            to identify the tls server itself
        :param sslMutualTLSCertificateAuthorityBundleFilePath: a filepath to a
                            PEM file that contains all CA certificates which are
                            used for mutualTLS to verify the identities of the
                            tls clients
        :param sslMutualTLSTrustedPeerCertificateBundleFilePath: a filepath that
                            loads all trusted peer certificates for mTLS when
                            `sslEnableMutualTLS` is enabled
        :return: None
        """
        logger.info(
            "Setting up Websocket Server '%s' at port %s, "
            "with ssl '%s'"
            "with client PEM bundle from '%s', "
            "with mTLS '%s', "
            "with mTLS CA bundle from '%s', "
            "with mTLS trusted peer bundle from '%s'",
            name,
            port,
            f"{'on' if enableSsl else 'off'}",
            sslBundleFilePath,
            f"{'on' if sslEnableMutualTLS else 'off'}",
            sslMutualTLSCertificateAuthorityBundleFilePath,
            sslMutualTLSTrustedPeerCertificateBundleFilePath,
        )

        vortexServer = VortexServer(name, requiresBase64Encoding=False)
        cls.__vortexServersByName[name].append(vortexServer)
        vortexWebsocketServerFactory = VortexWebsocketServerFactory(
            vortexServer
        )
        site = WebSocketFactory(vortexWebsocketServerFactory)
        if enableSsl:
            trustedCertificateAuthorities = None
            if sslEnableMutualTLS:
                trustedCertificateAuthorities = parseTrustRootFromBundle(
                    sslMutualTLSCertificateAuthorityBundleFilePath
                )
                trustedCertificateAuthorities = trustRootFromCertificates(
                    trustedCertificateAuthorities
                )

            privateKeyWithFullChain = parsePemBundleForServer(sslBundleFilePath)

            trustedPeerCertificates = None
            if sslMutualTLSTrustedPeerCertificateBundleFilePath is not None:
                trustedPeerCertificates = parsePemBundleForTrustedPeers(
                    sslMutualTLSTrustedPeerCertificateBundleFilePath
                )

            sslContextFactory = buildCertificateOptionsForTwisted(
                privateKeyWithFullChain,
                trustRoot=trustedCertificateAuthorities,
                trustedPeerCertificates=trustedPeerCertificates,
            )
            port = reactor.listenSSL(port, site, sslContextFactory)

        else:
            port = reactor.listenTCP(port, site)

        cls.__listeningPorts.append(port)

    @classmethod
    def createHttpWebsocketResource(
        cls, name: str
    ) -> VortexWebSocketUpgradeResource:
        vortexServer = VortexServer(name, requiresBase64Encoding=False)
        cls.__vortexServersByName[name].append(vortexServer)

        vortexWebsocketServerFactory = VortexWebsocketServerFactory(
            vortexServer
        )
        websocketFactory = VortexWrappedWebSocketFactory(
            vortexWebsocketServerFactory
        )

        return VortexWebSocketUpgradeResource(websocketFactory)

    @classmethod
    def createHttpWebsocketServer(cls, name: str, rootResource) -> None:
        """Create Server

        Create a vortex server, VortexServer clients connect to this vortex serer via HTTP(S)

        VortexServer clients will connect and provide their names. This allows the factory to
        abstract away the vortex UUIDs from their names.

        :param name: The name of the local vortex.
        :param port: The tcp port to listen on
        :return: None
        """

        vortexServer = VortexServer(name, requiresBase64Encoding=False)
        cls.__vortexServersByName[name].append(vortexServer)
        vortexWebsocketServerFactory = VortexWebsocketServerFactory(
            vortexServer
        )
        websocketFactory = VortexWrappedWebSocketFactory(
            vortexWebsocketServerFactory
        )

        websocketResource = VortexWebSocketUpgradeResource(websocketFactory)
        rootResource.putChild(b"vortexws", websocketResource)

    @classmethod
    def createTcpServer(cls, name: str, port: int) -> None:
        """Create Server

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
        port = reactor.listenTCP(port, vortexTcpServerFactory)
        cls.__listeningPorts.append(port)

    @classmethod
    def createHttpClient(cls, name: str, host: str, port: int) -> Deferred:
        """Create Client

        Connect to a vortex Server.

        :param name: The name of the local vortex.
        :param host: The hostname of the remote vortex.
        :param port: The port of the remote vortex.

        :return: A deferred from the VortexHttpClient.connect method
        """

        logger.info("Connecting to Peek Server HTTP %s:%s", host, port)

        vortexClient = VortexClientHttp(name)
        cls.__vortexClientsByName[name].append(vortexClient)

        return vortexClient.connect(host, port)

    @classmethod
    def createWebsocketClient(
        cls,
        name: str,
        host: str,
        port: int,
        url: str,
        sslEnableMutualTLS: bool = False,
        sslClientCertificateBundleFilePath: str = None,
        sslMutualTLSCertificateAuthorityBundleFilePath: str = None,
        sslMutualTLSTrustedPeerCertificateBundleFilePath: str = None,
    ) -> Deferred:
        """Create websocket client

        Connect to a vortex websocket server
        :param name: The name of the local vortex
        :param host: The hostname of the remote vortex
        :param port: The port of the remote vortex
        :param url: The websocket url that this client tries to connect to
        :param sslEnableMutualTLS: switch on or off mTLS
        :param sslClientCertificateBundleFilePath: a PEM bundle file that
                    contains a pair of key and certificate for this web client
        :param sslMutualTLSCertificateAuthorityBundleFilePath：CA bundle file path
                                            for TLS client authentication

        :param sslMutualTLSTrustedPeerCertificateBundleFilePath: a PEM bundle
                    file that contains certificates of all trusted peers. Each
                    certificate means a PEM with no intermediate CAs and/or
                    root CAs.
        :return: A deferred from the autobahn.twisted.connectWS method
        """
        logger.info(
            "Connecting to Peek Websocket Server %s:%s, "
            "with client PEM bundle from '%s', "
            "with mTLS '%s', "
            "with mTLS CA bundle from '%s', "
            "with mTLS trusted peer bundle from '%s' ",
            host,
            port,
            sslClientCertificateBundleFilePath,
            f"{'on' if sslEnableMutualTLS else 'off'}",
            sslMutualTLSCertificateAuthorityBundleFilePath,
            sslMutualTLSTrustedPeerCertificateBundleFilePath,
        )
        vortexWebsocketClientFactory = VortexClientWebsocketFactory(
            name, url=url
        )
        cls.__vortexClientsByName[name].append(vortexWebsocketClientFactory)

        if vortexWebsocketClientFactory.isSecure and sslEnableMutualTLS:
            trustedCertificateAuthorities = parseTrustRootFromBundle(
                sslMutualTLSCertificateAuthorityBundleFilePath
            )
            trustedCertificateAuthorities = trustRootFromCertificates(
                trustedCertificateAuthorities
            )

            privateKeyWithFullChain = parsePemBundleForClient(
                sslClientCertificateBundleFilePath
            )

            trustedPeerCertificates = None
            if sslMutualTLSTrustedPeerCertificateBundleFilePath is not None:
                trustedPeerCertificates = parsePemBundleForTrustedPeers(
                    sslMutualTLSTrustedPeerCertificateBundleFilePath
                )

            sslContextFactory = buildCertificateOptionsForTwisted(
                privateKeyWithFullChain,
                trustRoot=trustedCertificateAuthorities,
                trustedPeerCertificates=trustedPeerCertificates,
            )
        else:
            # use default for http or normal https
            sslContextFactory = ssl.ClientContextFactory()
            # raise minimum version to tls 1.2
            sslContextFactory.method = SSL.TLSv1_2_METHOD

        return vortexWebsocketClientFactory.connect(
            host, port, sslContextFactory
        )

    @classmethod
    def createTcpClient(cls, name: str, host: str, port: int) -> Deferred:
        """Create Client

        Connect to a vortex Server.

        :param name: The name of the local vortex.
        :param host: The hostname of the remote vortex.
        :param port: The port of the remote vortex.

        :return: A deferred from the VortexTcpClient.connect method
        """

        logger.info("Connecting to Peek Server TCP %s:%s", host, port)

        vortexClient = VortexClientTcp(name)
        cls.__vortexClientsByName[name].append(vortexClient)

        return vortexClient.connect(host, port)

    @classmethod
    def addCustomServerVortex(cls, vortex: VortexABC):
        cls.__vortexServersByName[vortex.localVortexInfo.name].append(vortex)

    @classmethod
    def isVortexNameLocal(cls, vortexName: str) -> bool:
        for vortex in cls._allVortexes():
            if vortex.localVortexInfo.name == vortexName:
                return True

        return False

    @classmethod
    def getLocalVortexClients(cls, localVortexName: str) -> List[VortexABC]:
        vortexes: List[VortexABC] = []

        for items in cls.__vortexClientsByName.values():
            vortexes.extend(
                filter(
                    lambda x: x.localVortexInfo.name == localVortexName, items
                )
            )

        return vortexes

    @classmethod
    def getRemoteClientVortexInfos(cls) -> List[VortexInfo]:
        clientVortexInfos = []

        for vortexes in cls.__vortexServersByName.values():
            for vortex in vortexes:
                clientVortexInfos.extend(vortex.remoteVortexInfo)

        return clientVortexInfos

    @classmethod
    def getRemoteVortexUuids(cls) -> List[str]:
        remoteUuids = []

        for vortex in cls._allVortexes():
            for remoteVortexInfo in vortex.remoteVortexInfo:
                remoteUuids.append(remoteVortexInfo.uuid)

        return remoteUuids

    @classmethod
    def getRemoteVortexName(cls) -> List[str]:
        remoteNames = set()

        for vortex in cls._allVortexes():
            for remoteVortexInfo in vortex.remoteVortexInfo:
                remoteNames.add(remoteVortexInfo.name)

        return list(remoteNames)

    @classmethod
    def getRemoteVortexInfoByIp(cls, ip: str, vortexName=None) -> str:
        # Ignore the port if any
        ip = ip.split(":")[0]

        from vortex.VortexConnectionABC import VortexConnectionABC

        targetConns: list[VortexConnectionABC] = []
        for servers in cls.__vortexServersByName.values():
            for server in servers:
                for conn in server.connections.values():
                    if (
                        not conn.closed
                        and not conn.timedOut
                        and conn.ip == ip
                        and (
                            conn.remoteVortexName == vortexName
                            or vortexName is None
                        )
                    ):
                        targetConns.append(conn)

        if len(targetConns) > 1:
            logger.warning(
                "Multiple vortexes found for ip=%s, name=%s,"
                " choosing latest connection",
                ip,
                vortexName,
            )
            targetConns.sort(key=lambda c: c.connectDateTime)
            # As of Python3.6, dict are ordered, so the newest is the last.
            return targetConns[-1].remoteVortexUuid

        if not targetConns:
            raise NoVortexException(f"Can not find vortex with IP {ip}")

        return targetConns[0].remoteVortexUuid

    @classmethod
    @inlineCallbacks
    def sendVortexMsg(
        cls,
        vortexMsgs: Union[VortexMsgList, bytes],
        destVortexName: Optional[str] = broadcast,
        destVortexUuid: Optional[str] = broadcast,
    ) -> Deferred:
        """Send VortexMsg

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
            raise NoVortexException(
                "Can not find vortexes to send message to,"
                " name=%s, uuid=%s" % (destVortexName, destVortexUuid)
            )

        if not isinstance(vortexMsgs, list):
            vortexMsgs = [vortexMsgs]

        base64VortexMsgs = vortexMsgs[:]

        # If any transports require base64 encoding, then encode them all
        if [v for v in vortexAndUuids if v[0].requiresBase64Encoding]:
            for index, vortexMsg in enumerate(base64VortexMsgs):
                if vortexMsg.startswith(b"{"):
                    base64VortexMsgs[
                        index
                    ] = yield PayloadEnvelope.base64EncodeDefer(vortexMsg)

        deferreds = []
        for vortex, uuids in vortexAndUuids:
            for uuid in uuids:
                msgToUse = (
                    base64VortexMsgs
                    if vortex.requiresBase64Encoding
                    else vortexMsgs
                )
                deferreds.append(vortex.sendVortexMsg(msgToUse, uuid))

        results = yield DeferredList(deferreds)
        return results

    @classmethod
    def sendVortexMsgLocally(
        cls,
        vortexMsgs: Union[VortexMsgList, bytes],
        priority: int = DEFAULT_PRIORITY,
    ) -> Deferred:
        """Send VortexMsg

        Sends a payload to the remote vortex.

        :param vortexMsgs: The vortex message(s) to deliver locally.

        :return: A C{Deferred} which will callback when the message has been delivered.
        """
        yesMainThread()

        vortexUuid = "local"
        vortexName = "local"
        httpSession = "local"
        sendResponse = VortexFactory.sendVortexMsgLocally

        vortexMsgs = (
            [vortexMsgs] if isinstance(vortexMsgs, bytes) else vortexMsgs
        )

        def send(payloadEnvelope: PayloadEnvelope):
            try:
                PayloadIO().process(
                    payloadEnvelope=payloadEnvelope,
                    vortexUuid=vortexUuid,
                    vortexName=vortexName,
                    httpSession=httpSession,
                    sendResponse=sendResponse,
                )
                return succeed(True)

            except Exception as e:
                return Failure(e)

        deferreds = []
        for vortexMsg in vortexMsgs:
            d = PayloadEnvelope().fromVortexMsgDefer(vortexMsg)
            d.addCallback(send)
            deferreds.append(d)

        return DeferredList(deferreds)

    @classmethod
    def subscribeToVortexStatusChange(cls, vortexName: str) -> Subject:
        """Subscribe to Vortex Status Change

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
    def _notifyOfVortexStatusChange(cls, vortexName: str, online: bool) -> None:
        if cls.__isShutdown:
            return

        logger.debug(
            "Vortex %s went %s", vortexName, ("online" if online else "offline")
        )

        if vortexName in cls.__vortexStatusChangeSubjectsByName:
            cls.__vortexStatusChangeSubjectsByName[vortexName].on_next(online)

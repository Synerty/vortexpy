"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging
import uuid
from typing import Optional, Union, Dict, List, Any
from weakref import WeakValueDictionary

from twisted.internet import task, reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.python.components import registerAdapter
from twisted.web.server import Session
from zope.interface import Interface, Attribute
from zope.interface.declarations import implementer

from .DeferUtil import vortexLogFailure, isMainThread
from .PayloadEnvelope import VortexMsgList, PayloadEnvelope
from .PayloadFilterKeys import rapuiServerEcho
from .PayloadIO import PayloadIO
from .PayloadPriority import DEFAULT_PRIORITY
from .VortexABC import VortexABC, VortexInfo

logger = logging.getLogger(__name__)

""" ---------------------------------------------------------------------------
VortexServer
"""

HEART_BEAT_PERIOD = 5.0
HEART_BEAT_TIMEOUT = 35.0


class VortexServer(VortexABC):
    """VortexServer
    The static instance of the controller
    """

    def __init__(self, name: str, requiresBase64Encoding=True) -> None:
        # Simple initialisations up the top
        self._name = name
        self._requiresBase64Encoding = requiresBase64Encoding
        self._uuid = str(uuid.uuid1())
        self._shutdown = False

        # Store all our sessions
        self._httpSessionsBySessionUuid: WeakValueDictionary = (
            WeakValueDictionary()
        )
        self._connectionByVortexUuid: Dict[str, Any] = {}

    def name(self):
        return self._name

    def uuid(self):
        return self._uuid

    @property
    def requiresBase64Encoding(self):
        return self._requiresBase64Encoding

    @property
    def localVortexInfo(self) -> VortexInfo:
        return VortexInfo(name=self._name, uuid=self._uuid)

    @property
    def remoteVortexInfo(self) -> List[VortexInfo]:
        vortexInfos = []

        for conn in self._connectionByVortexUuid.values():
            vortexInfos.append(
                VortexInfo(
                    name=conn.remoteVortexName, uuid=conn.remoteVortexUuid
                )
            )

        return vortexInfos

    def isShutdown(self):
        return self._shutdown

    def shutdown(self):
        self._shutdown = True

        for sess in list(self._httpSessionsBySessionUuid.values()):
            sess.expire()

        for conn in list(self._connectionByVortexUuid.values()):
            conn.close()

    def vortexUuids(self):
        return list(self._connectionByVortexUuid.keys())

    def isVortexAlive(self, vortexUuid):
        return vortexUuid in self._connectionByVortexUuid

    def vortexClientIpPort(self, vortexUuid):
        connection = self._connectionByVortexUuid.get(vortexUuid)
        if not connection:
            return None

        return "%s:%s" % (connection.ip, connection.port)

    def connectionOpened(self, httpSession, vortexConnection):
        # print "VortexServer - connectionOpened"

        vortexUuid = vortexConnection.remoteVortexUuid
        assert vortexUuid

        if httpSession:
            # If this is a new session, Make sure we have an expire callback on it
            if httpSession.uid not in self._httpSessionsBySessionUuid:
                self._httpSessionsBySessionUuid[httpSession.uid] = httpSession
                httpSession.notifyOnExpire(
                    lambda: self._sessionExpired(httpSession.uid)
                )

            # Update the connection dict in the sessions object
            httpSessionConns = VortexSessionI(httpSession).connections
            httpSessionConns[vortexUuid] = vortexConnection

        # Get the old connection if it exists
        if vortexUuid in self._connectionByVortexUuid:
            self._connectionByVortexUuid[vortexUuid].close()

        # Update the _connectionsByvortexUuid
        self._connectionByVortexUuid[vortexUuid] = vortexConnection

    def connectionClosed(self, conn):
        # print "VortexServer - connectionClosed"

        vortexUuid = conn.remoteVortexUuid

        if conn.httpSessionUuid in self._httpSessionsBySessionUuid:
            session = self._httpSessionsBySessionUuid[conn.httpSessionUuid]
            assert session

            # cleanup _sessionsBySessionUuid
            conns = VortexSessionI(session).connections
            if conns[vortexUuid] == conn:
                del conns[vortexUuid]

        # cleanup _connectionsByvortexUuid
        if conn.remoteVortexUuid in self._connectionByVortexUuid:
            if self._connectionByVortexUuid[conn.remoteVortexUuid] == conn:
                del self._connectionByVortexUuid[conn.remoteVortexUuid]

    def _sessionExpired(self, httpSessionUuid):
        logger.debug(
            "VortexServer - _sessionExpired, Session %s has expired"
            % httpSessionUuid
        )

        # cleanup _sessionsBySessionUuid
        del self._httpSessionsBySessionUuid[httpSessionUuid]

        # cleanup _connectionsByvortexUuid
        for vortexUuid, conn in list(self._connectionByVortexUuid.items()):
            if conn.httpSessionUuid == httpSessionUuid:
                del self._connectionByVortexUuid[vortexUuid]

    def payloadReveived(self, httpSession, vortexUuid, vortexName, payload):
        # print "VortexServer - payloadReveived"

        if rapuiServerEcho in payload.filt:
            payload.filt.pop(rapuiServerEcho)
            self.sendVortexMsg(payload.toVortexMsg(), vortexUuid)

        def sendResponse(
            vortexMsg: bytes, priority: int = DEFAULT_PRIORITY
        ) -> Deferred:
            """Send Back

            Sends a response back to where this payload come from.

            """
            return self.sendVortexMsg(
                vortexMsg, vortexUuid=vortexUuid, priority=priority
            )

        PayloadIO().process(
            payload,
            vortexUuid=vortexUuid,
            vortexName=vortexName,
            httpSession=httpSession,
            sendResponse=sendResponse,
        )

    def sendVortexMsg(
        self,
        vortexMsgs: Union[VortexMsgList, bytes, None] = None,
        vortexUuid: Optional[str] = None,
        priority: int = DEFAULT_PRIORITY,
    ):
        """Send Vortex Msg

        Sends the vortex message to any conencted clients with vortexUuid.
        Or broadcast it to all connected vortex clients if it's None

        :param vortexMsgs: The vortex message to send
        :param vortexUuid: The vortexUuid of the client to send to.
        """
        if vortexMsgs is None:
            vortexMsgs = [PayloadEnvelope().toVortexMsg()]

        if isMainThread():
            return self._sendVortexMsgLater(
                vortexMsgs, vortexUuid=vortexUuid, priority=priority
            )

        return task.deferLater(
            reactor,
            0,
            self._sendVortexMsgLater,
            vortexMsgs,
            vortexUuid=vortexUuid,
            priority=priority,
        )

    @inlineCallbacks
    def _sendVortexMsgLater(
        self,
        vortexMsgs: Union[VortexMsgList, bytes],
        vortexUuid: Optional[str],
        priority: int,
    ):
        """Send the message.

        Send it later,
        This also means it doesn't matter what thread this is called from

        """
        yield None

        if not isinstance(vortexMsgs, list):
            vortexMsgs = [vortexMsgs]

        # Deliver locally
        if vortexUuid == self._uuid:
            for vortexMsg in vortexMsgs:

                def sendResponse(vortexMsg_, priority_=DEFAULT_PRIORITY):
                    self._sendVortexMsgLater(vortexMsg_, self._uuid, priority_)

                def cb(payloadEnvelope: PayloadEnvelope) -> None:
                    PayloadIO().process(
                        payloadEnvelope,
                        vortexUuid=self._uuid,
                        vortexName=self._name,
                        httpSession=None,
                        sendResponse=sendResponse,
                    )

                d = PayloadEnvelope.fromVortexMsgDefer(vortexMsg)
                d.addCallback(cb)
                d.addErrback(vortexLogFailure, logger, consumeError=True)

            return

        from .VortexServerConnection import VortexServerConnection

        # If any transports require base64 encoding, then encode them all
        if self.requiresBase64Encoding:
            for index, vortexMsg in enumerate(vortexMsgs):
                if vortexMsg.startswith(b"{"):
                    vortexMsgs[index] = yield PayloadEnvelope.base64EncodeDefer(
                        vortexMsg
                    )

        conns: List[VortexServerConnection] = []
        if vortexUuid is None:
            conns = list(self._connectionByVortexUuid.values())
        elif vortexUuid in self._connectionByVortexUuid:
            conns.append(self._connectionByVortexUuid[vortexUuid])

        for conn in conns:
            for vortexMsg in vortexMsgs:
                conn.write(vortexMsg, priority)

        return True


""" ---------------------------------------------------------------------------
VortexServer Session
"""


class VortexSessionI(Interface):
    connections: Dict = Attribute(
        "VortexServer connections for this session, by window uuid"
    )


@implementer(VortexSessionI)
class VortexSession(object):
    def __init__(self, session):
        session.sessionTimeout = HEART_BEAT_TIMEOUT
        self.connections = {}


registerAdapter(VortexSession, Session, VortexSessionI)

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
from weakref import WeakValueDictionary

from twisted.internet import task, reactor
from twisted.python.components import registerAdapter
from twisted.web.server import Session
from zope.interface import Interface, Attribute
from zope.interface.declarations import implementer

from vortex.Payload import Payload
from vortex.PayloadFilterKeys import rapuiServerEcho
from vortex.PayloadIO import PayloadIO

logger = logging.getLogger(__name__)

''' ---------------------------------------------------------------------------
Vortex
'''


class Vortex(object):
    ''' Vortex
    The static instance of the controller
    '''

    # Singleton
    _instance = None

    HEART_BEAT_PERIOD = 5.0
    HEART_BEAT_TIMEOUT = 35.0

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(Vortex, cls).__new__(cls)
            cls._instance.__singleton_init__()
        return cls._instance

    def __singleton_init__(self):
        # Simple initialisations up the top
        self._uuid = str(uuid.uuid1())
        self._shutdown = False

        # Store all our sessions
        self._httpSessionsBySessionUuid = WeakValueDictionary()
        self._connectionByVortexUuid = {}

        # Start our heart beat
        self._beatLoopingCall = task.LoopingCall(self._beat)
        self._beatLoopingCall.start(self.HEART_BEAT_PERIOD)

    def uuid(self):
        return self._uuid

    def isShutdown(self):
        return self._shutdown

    def shutdown(self):
        self._shutdown = True

        if self._beatLoopingCall.running:
            self._beatLoopingCall.stop()

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

    def _beat(self):
        # print "Vortex - beat"
        # Send the heartbeats
        self.send(Payload())

        # Make sure we only touch the sessions that have connections
        httpSessionUuids = set([conn.httpSessionUuid
                            for conn in list(self._connectionByVortexUuid.values())])

        # Touch all the sessions
        for sess in list(self._httpSessionsBySessionUuid.values()):
            if sess and sess.uid in httpSessionUuids:
                sess.touch()

    def connectionOpened(self, httpSession, vortexConnection):
        # print "Vortex - connectionOpened"

        vortexUuid = vortexConnection.vortexUuid
        assert vortexUuid

        if httpSession:
            # If this is a new session, Make sure we have an expire callback on it
            if httpSession.uid not in self._httpSessionsBySessionUuid:
                self._httpSessionsBySessionUuid[httpSession.uid] = httpSession
                httpSession.notifyOnExpire(lambda: self._sessionExpired(httpSession.uid))

            # Update the connection dict in the sessions object
            httpSessionConns = VortexSessionI(httpSession).connections
            httpSessionConns[vortexUuid] = vortexConnection

        # Get the old connection if it exists
        if vortexUuid in self._connectionByVortexUuid:
            self._connectionByVortexUuid[vortexUuid].close()

        # Update the _connectionsByvortexUuid
        self._connectionByVortexUuid[vortexUuid] = vortexConnection

        # Send a heart beat down the new connection, tell it who we are.
        connectPayloadFilt = {}
        connectPayloadFilt[Payload.vortexUuidKey] = self._uuid
        self.send(Payload(filt=connectPayloadFilt), vortexUuid)

    def connectionClosed(self, conn):
        # print "Vortex - connectionClosed"

        vortexUuid = conn.vortexUuid

        if conn.httpSessionUuid in self._httpSessionsBySessionUuid:
            session = self._httpSessionsBySessionUuid[conn.httpSessionUuid]
            assert session

            # cleanup _sessionsBySessionUuid
            conns = VortexSessionI(session).connections
            if conns[vortexUuid] == conn:
                del conns[vortexUuid]

        # cleanup _connectionsByvortexUuid
        if conn.vortexUuid in self._connectionByVortexUuid:
            if self._connectionByVortexUuid[conn.vortexUuid] == conn:
                del self._connectionByVortexUuid[conn.vortexUuid]

    def _sessionExpired(self, httpSessionUuid):
        logger.debug("Vortex - _sessionExpired, Session %s has expired" % httpSessionUuid)

        # cleanup _sessionsBySessionUuid
        del self._httpSessionsBySessionUuid[httpSessionUuid]

        # cleanup _connectionsByvortexUuid
        for vortexUuid, conn in list(self._connectionByVortexUuid.items()):
            if conn.httpSessionUuid == httpSessionUuid:
                del self._connectionByVortexUuid[vortexUuid]

    def payloadReveived(self, session, conn, payload):
        # print "Vortex - payloadReveived"

        if rapuiServerEcho in payload.filt:
            payload.filt.pop(rapuiServerEcho)
            self.send(payload, conn.vortexUuid)

        PayloadIO().process(payload,
                            vortexUuid=conn.vortexUuid,
                            vtSessionUuid=conn.httpSessionUuid,
                            vtSession=session,
                            session=session  # OLD
                            )

    def send(self, payload, vortexUuid=None):
        reactor.callLater(0, self._sendLater, payload, vortexUuid)

    def _sendLater(self, payload, vortexUuid):
        # TODO, Rename to vortexUuid
        if vortexUuid == self._uuid:
            PayloadIO().process(payload)
            return

        self.sendVortexMsg(payload.toVortexMsg(), vortexUuid=vortexUuid)

    def sendVortexMsg(self, vortexMsg, vortexUuid=None):
        reactor.callLater(0, self._sendVortexMsgLater, vortexMsg,
                          vortexUuid=vortexUuid)

    def _sendVortexMsgLater(self, vortexMsg, vortexUuid=None):
        ''' Send the message.
        Send it later,
        This also means it doesn't matter what thread this is called from
        '''
        conns = []
        if vortexUuid == None:
            conns = list(self._connectionByVortexUuid.values())
        elif vortexUuid in self._connectionByVortexUuid:
            conns.append(self._connectionByVortexUuid[vortexUuid])

        for conn in conns:
            conn.write(vortexMsg)


def vortexSendTuple(filt, tuple_, vortexUuid=None):
    if not "key" in filt:
        raise Exception("There is no 'key' in the payload filt"
                        ", There must be one for routing")

    vortexMsg = Payload(filt=filt, tuples=tuple_).toVortexMsg()
    Vortex().sendVortexMsg(vortexMsg, vortexUuid=vortexUuid)


def vortexSendResult(filt, result, vortexUuid=None):
    if not "key" in filt:
        raise Exception("There is no 'key' in the payload filt"
                        ", There must be one for routing")

    vortexMsg = Payload(filt=filt, result=result).toVortexMsg()
    Vortex().sendVortexMsg(vortexMsg, vortexUuid=vortexUuid)


def vortexSendPayload(payload, vortexUuid=None):
    if not "key" in payload.filt:
        raise Exception("There is no 'key' in the payload filt"
                        ", There must be one for routing")

    vortexMsg = payload.toVortexMsg()
    Vortex().sendVortexMsg(vortexMsg, vortexUuid=vortexUuid)


def vortexSendVortexMsg(payloadB64encodeXml, vortexUuid=None):
    Vortex().sendVortexMsg(payloadB64encodeXml, vortexUuid=vortexUuid)


def vortexIsClientAlive(vortexUuid):
    return Vortex().isVortexAlive(vortexUuid)


def vortexClientIpPort(vortexUuid):
    return Vortex().vortexClientIpPort(vortexUuid)


''' ---------------------------------------------------------------------------
Vortex Session
'''


class VortexSessionI(Interface):
    connections = Attribute("Vortex connections for this session, by window uuid")

@implementer(VortexSessionI)
class VortexSession(object):

    def __init__(self, session):
        session.sessionTimeout = Vortex.HEART_BEAT_TIMEOUT
        self.connections = {}


registerAdapter(VortexSession, Session, VortexSessionI)

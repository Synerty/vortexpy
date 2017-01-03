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
from datetime import datetime
from http.cookiejar import CookieJar
from typing import Union, Optional
from urllib.parse import urlencode

from twisted.internet import reactor, task
from twisted.internet.defer import succeed, Deferred
from twisted.web.client import Agent, CookieAgent
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface.declarations import implementer

from vortex.Payload import Payload, VortexMsgList
from vortex.VortexABC import VortexABC, VortexInfo
from vortex.VortexPayloadClientProtocol import VortexPayloadClientProtocol

logger = logging.getLogger(name=__name__)


@implementer(IBodyProducer)
class _VortexClientPayloadProducer(object):
    def __init__(self, vortexMsgs):
        self.vortexMsgs = b''

        for vortexMsg in vortexMsgs:
            self.vortexMsgs += vortexMsg + b"."

        self.length = len(self.vortexMsgs)

    def startProducing(self, consumer):
        consumer.write(self.vortexMsgs)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class VortexClient(VortexABC):
    """ VortexServer Client
    Connects to a votex server
    """

    def __init__(self, name: str):
        self._vortexName = name
        self._vortexUuid = str(uuid.uuid1())

        self._server = None
        self._port = None

        self._connectionBroken = False

        self._cookieJar = CookieJar()

        self._beatTime = None
        self._beatTimeout = 15.0  # Server beats at 5 seconds

        # Start our heart beat checker
        self._beatLoopingCall = task.LoopingCall(self._checkBeat)

        self._reconnectVortexMsgs = [Payload().toVortexMsg()]

        self.__protocol = None

    @property
    def localVortexInfo(self) -> VortexInfo:
        return VortexInfo(name=self._vortexName,
                          uuid=self._vortexUuid)

    @property
    def remoteVortexInfo(self) -> [VortexInfo]:
        if not self.__protocol:
            return []

        if not self.__protocol.serverVortexUuid:
            return []

        return [VortexInfo(name=self.__protocol.serverVortexName,
                           uuid=self.__protocol.serverVortexUuid)]

    @property
    def name(self):
        return self._vortexName

    def connect(self, server, port):
        self._server = server
        self._port = port

        self._beat()
        self._beatLoopingCall.start(5.0)
        return self.sendVortexMsg()

    def disconnect(self):
        self.__protocol.transport.loseConnection()

    def addReconnectPayload(self, payload):
        """ Add Reconnect Payload
        :param payload: Payload to send when the connection reconnects
        :return:
        """
        self._reconnectVortexMsgs.append(payload.toVortexMsg())

    def sendVortexMsg(self,
                      vortexMsgs: Union[VortexMsgList, bytes, None] = None,
                      vortexUuid: Optional[str] = None) -> Deferred:

        if vortexMsgs is None:
            vortexMsgs = self._reconnectVortexMsgs

        if not isinstance(vortexMsgs, list):
            vortexMsgs = [vortexMsgs]

        # Check if the vortexUuid matches the destination uuid
        #
        # if vortexUuid and vortexUuid != self._
        #
        # if not self.__protocol.serverVortexUuid:
        #     return []

        return task.deferLater(reactor, 0, self._sendVortexMsgLater, vortexMsgs)

    def _sendVortexMsgLater(self, vortexMsgs: VortexMsgList):
        assert self._server
        assert vortexMsgs

        if not isinstance(vortexMsgs, list):
            vortexMsgs = [vortexMsgs]

        def cbRequest(response):
            if response.code != 200:
                logger.error("Connection to vortex %s:%s failed",
                             self._server, self._port)
                return False

            elif self._connectionBroken:
                logger.info("VortexServer client %s:%s reconnected",
                            self._server, self._port)

            self._connectionBroken = False
            self.__protocol = VortexPayloadClientProtocol(logger, vortexClient=self)
            response.deliverBody(self.__protocol)
            return True

        bodyProducer = _VortexClientPayloadProducer(vortexMsgs)

        agent = CookieAgent(Agent(reactor), self._cookieJar)

        args = {
            'vortexUuid': self._vortexUuid,
            'vortexName': self._vortexName
        }

        uri = ("http://%s:%s/vortex?%s"
               % (self._server, self._port, urlencode(args))).encode("UTF-8")

        d = agent.request(
            b'POST', uri,
            Headers({b'User-Agent': [b'Synerty VortexServer Client'],
                     b'Content-Type': [b'text/plain']}),
            bodyProducer)

        d.addCallback(cbRequest)
        return d

    def _beat(self):
        self._beatTime = datetime.utcnow()

    def _checkBeat(self):
        if not (datetime.utcnow() - self._beatTime).seconds > self._beatTimeout:
            return

        self._connectionBroken = True
        logger.info("VortexServer client dead, reconnecting %s:%s"
                    % (self._server, self._port))

        d = self.sendVortexMsg()

        # Add a errback that handles the failure.
        d.addErrback(lambda _: None)

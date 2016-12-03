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
from http.cookiejar import CookieJar
from datetime import datetime
from urllib.parse import urlencode

from rapui.DeferUtil import printFailure, deferToThreadWrap
from PayloadIO import PayloadIO
from twisted.internet import reactor, task
from twisted.internet.defer import succeed, inlineCallbacks
from twisted.internet.protocol import Protocol
from twisted.web._newclient import ResponseDone, ResponseNeverReceived
from twisted.web.client import Agent, CookieAgent
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface.declarations import implements

from Payload import Payload

logger = logging.getLogger(name=__name__)


class VortexPayloadServerProtocol(Protocol):
    def __init__(self):
        self._data = ""

    @inlineCallbacks
    def _processData(self):
        if not self._data:
            return

        def getNextChunk():
            try:
                return self._data.index('.')
            except ValueError:
                # There is no '.' in it, wait for more data.
                return None

        nextChunk = getNextChunk()

        while nextChunk:
            vortexMsg = self._data[:nextChunk]
            self._data = self._data[nextChunk + 1:]

            try:
                payload = yield Payload().fromVortexMsgDefer(vortexMsg)

                if payload.isEmpty():
                    return

                PayloadIO().process(payload, vortexUuid=self._vortexUuid)

            except Exception as e:
                self._logger.exception(e)
                raise

            nextChunk = getNextChunk()

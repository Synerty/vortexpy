import gzip
import logging
import struct
from datetime import datetime
from io import BytesIO
from typing import Union, List, Type

import pytz
from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.defer import succeed
from twisted.internet.protocol import Protocol
from twisted.web._newclient import ResponseDone
from twisted.web.client import Agent, GzipDecoder, ContentDecoderAgent
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer, UNKNOWN_LENGTH
from zope.interface import implementer

from vortex.Tuple import Tuple
from vortex.restful.RestfulResource import HTTP_REQUEST

logger = logging.getLogger(__name__)


class GzippedDataHttpClient:
    """A Restful http client that sends out a binary payload"""

    DEFAULT_HEADERS = {
        "User-Agent": ["vortex/1.0"],
        "Content-Type": ["application/json"],
    }

    def __init__(
        self,
        url: Union[str, bytes],
        payload: bytes,
        headers: dict[str, List[str]],
        method: HTTP_REQUEST,
        meta: Type[Tuple],
        isPayloadGzipped: bool = True,
        compressed: bool = True,
        timeout: float = 10,
    ):
        self._httpMethod = method.value.encode()
        self._url = url.encode() if isinstance(url, str) else url
        self._payload = payload
        self._headers = {**self.DEFAULT_HEADERS, **headers}
        self._meta = meta
        self._compressed = (
            compressed  # send http requests whether gzip-compressed or not
        )
        self._isPayloadGzipped = isPayloadGzipped
        self._timeout = timeout  # connect timeout

    @inlineCallbacks
    def run(self) -> Type[Tuple]:
        agent = Agent(reactor, connectTimeout=self._timeout)
        # Add the gzip decoder
        if self._compressed:
            agent = ContentDecoderAgent(agent, [(b"gzip", GzipDecoder)])

        binaryPayloadRequestProducer = _BinaryPayloadRequestProducer(
            self._payload,
            self._meta,
            self._isPayloadGzipped,
        )
        # Make the web request
        try:
            response = yield agent.request(
                self._httpMethod,
                self._url,
                Headers(self._headers),
                binaryPayloadRequestProducer,
            )
            self._meta = binaryPayloadRequestProducer.meta
        except Exception as e:
            self._meta.exceptions.append(e)
        finally:
            if self._meta.exceptions:
                return self._meta

        self._meta.code = response.code
        self._meta.version = response.version
        self._meta.headers = {
            k.decode(): v[0].decode()
            for k, v in response.headers.getAllRawHeaders()
        }

        self._meta = yield self._cbResponse(response, self._meta)
        return self._meta

    def _cbResponse(self, response, meta):
        responseProducer = _ResponseProducer(meta)
        response.deliverBody(responseProducer)
        return responseProducer.asyncMeta


@implementer(IBodyProducer)
class _BinaryPayloadRequestProducer:
    def __init__(
        self,
        payload: bytes,
        meta: Type[Tuple],
        isPayloadGzipped: bool = True,
    ):
        self._meta = meta
        self._payload = payload
        self._isGzippedAlready = isPayloadGzipped
        self.length = self._getGzipUncompressedSize()
        self._gzipFile = None

    @property
    def meta(self):
        return self._meta

    def startProducing(self, consumer):
        self._meta.requestDate = datetime.now(tz=pytz.utc)

        payload = self._payload
        if self._isGzippedAlready:
            payload = gzip.decompress(self._payload)

        try:
            consumer.write(payload)
        except Exception as e:
            raise e

        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass

    def _getGzipUncompressedSize(self) -> int:
        # Uncompressed size is stored in the last 4 bytes of the gzip file.
        # We can read the binary data and convert it to an int.
        # (This will only work for files under 4GB)
        if not self._isGzippedAlready:
            return len(self._payload)

        with BytesIO(self._payload) as f:
            try:
                return struct.unpack("I", self._payload[-4:])[0]
            except Exception as e:
                # may be invalid format of gzip
                logger.exception(e)
                return UNKNOWN_LENGTH


class _ResponseProducer(Protocol):
    def __init__(self, meta: Type[Tuple]):
        self._finishedDeferred = Deferred()
        self._writeSize = 0
        self._body = b""
        self._meta = meta

    @property
    def asyncMeta(self):
        return self._finishedDeferred

    def dataReceived(self, data: bytes):
        self._body += data

    def connectionLost(self, reason):
        if isinstance(reason.value, ResponseDone):
            self._meta.responseDate = datetime.now(tz=pytz.utc)
            self._meta.body = self._body
            self._finishedDeferred.callback(self._meta)

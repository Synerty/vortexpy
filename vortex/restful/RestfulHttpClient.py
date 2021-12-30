import json
import logging
from typing import Union, Type, runtime_checkable

from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.protocol import Protocol
from twisted.web._newclient import ResponseDone
from twisted.web.client import Agent, GzipDecoder, ContentDecoderAgent
from twisted.web.http_headers import Headers
from zope.interface import implementer
from twisted.internet.defer import succeed
from twisted.web.iweb import IBodyProducer

from vortex.Tuple import Tuple
from vortex.restful.RestfulResource import HTTP_REQUEST

logger = logging.getLogger(__name__)


class RestfulHttpClient:
    def __init__(
        self,
        url: Union[str, bytes],
        method: HTTP_REQUEST,
        postTuple: Tuple,
        ResponseTuple: Type[Tuple],
    ):
        self._httpMethod = method.value.encode()
        self._url = url.encode() if isinstance(url, str) else url
        self._postTuple = postTuple
        self._ResponseTuple = ResponseTuple

    @inlineCallbacks
    def run(self):
        # convert tuple to json
        body = self._postTuple.tupleToRestfulJsonDict()
        body = json.dumps(body).encode("utf-8")

        # add http headers
        headers = Headers(
            {
                "User-Agent": ["synerty/1.0"],
                "Content-Type": ["application/json"],
            }
        )

        # Add the gzip decoder
        agent = ContentDecoderAgent(Agent(reactor), [(b"gzip", GzipDecoder)])

        # Make the web request
        response = yield agent.request(
            self._httpMethod, self._url, headers, _BytesProducer(body)
        )

        # Get the response data
        responseData = yield self._cbResponse(response)

        # Convert the bytes into a tuple and return
        return self._parseTuple(responseData)

    def _cbResponse(self, response):
        bodyDownloader = _RestfulBody()
        response.deliverBody(bodyDownloader)
        return bodyDownloader.asyncData

    def _parseTuple(self, bytes_):
        json_ = json.loads(bytes_.decode("utf-8"))
        return Tuple.restfulJsonDictToTupleWithValidation(
            json_, self._ResponseTuple
        )


@implementer(IBodyProducer)
class _BytesProducer:
    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class _RestfulBody(Protocol):
    def __init__(self):
        self._finishedDeferred = Deferred()
        self._writeSize = 0
        self._body = b""

    @property
    def asyncData(self):
        return self._finishedDeferred

    def dataReceived(self, data: bytes):
        logger.debug(bytes)
        self._body += data

    def connectionLost(self, reason):
        if isinstance(reason.value, ResponseDone):
            self._finishedDeferred.callback(self._body)

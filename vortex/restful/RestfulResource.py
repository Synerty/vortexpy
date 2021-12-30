import json
import logging
from enum import Enum
from typing import Callable
from typing import List

from twisted.internet.defer import inlineCallbacks
from twisted.python.compat import nativeString
from twisted.web.server import NOT_DONE_YET
from txhttputil.site.BasicResource import BasicResource
from vortex import Tuple
from vortex.DeferUtil import deferToThreadWrapWithLogger
from vortex.DeferUtil import vortexLogFailure

logger = logging.getLogger(__name__)


class HTTP_REQUEST(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class _JsonResource(BasicResource):
    isLeaf = True
    isGzipped = True

    def __init__(self, handler: Callable, TupleClass: Tuple):
        BasicResource.__init__(self)

        self._handler = handler
        self._tupleClass = TupleClass

    @deferToThreadWrapWithLogger(logger)
    def _requestToTuple(self, request) -> Tuple:
        req = json.load(request.content)
        tuple_ = self._tupleClass()
        tuple_.fromJsonDict(req)
        return tuple_

    @deferToThreadWrapWithLogger(logger)
    def _tupleToResponse(self, tuple_: Tuple) -> bytes:
        dict_ = tuple_.tupleToRestfulJsonDict()
        json_ = json.dumps(dict_)
        return json_.encode()

    def _writeSuccessResponse(self, response: bytes, request):
        request.setResponseCode(200)
        request.setHeader("Content-Type", "application/json")
        request.write(response)
        request.finish()

    def _writeErrorResponse(self, request):
        request.setResponseCode(500)
        request.setHeader("Content-Type", "application/json")
        request.write(b'{"error":"internal error"}')
        request.finish()

    @inlineCallbacks
    def _renderAsync(self, request):
        tupleIn = None
        try:
            tupleIn = yield self._requestToTuple(request)
            tupleOut = yield self._handler(tupleIn)
            response = yield self._tupleToResponse(tupleOut)
            self._writeSuccessResponse(response, request)

        except Exception as e:
            self._writeErrorResponse(request)
            logger.debug(
                f"Error while processing REST Tuple\n{tupleIn}\n{request.path}"
            )
            logger.exception(e)

    def render(self, request):
        d = self._renderAsync(request)
        d.addErrback(vortexLogFailure, logger=logger, consumeError=True)

        return NOT_DONE_YET


class ErrorJsonResource(BasicResource):
    isLeaf = True
    isGzipped = True

    def __init__(self, errorCode: int):
        self._errorCode = errorCode

    def render(self, request):
        request.setResponseCode(self._errorCode)
        return b""


class PluginRestfulResource(BasicResource):
    def __init__(self):
        BasicResource.__init__(self)
        self._registeredMethods = {}

    def registerMethod(
        self,
        handlerFunction: Callable,
        TupleClass: Tuple,
        urlPrefix: bytes,
        registeredRequestMethod: List[HTTP_REQUEST],
    ):
        if urlPrefix in self._registeredMethods:
            raise ValueError(f'Route "{urlPrefix}" already exists')
        if not callable(handlerFunction):
            raise TypeError(f'"{str(handlerFunction)}" is not a callable')

        # register handler as partial function (poor man's Template<type>)
        # as callback function whose input is an instance of a Tuple
        self._registeredMethods[urlPrefix] = {
            "handler": handlerFunction,
            "allowedMethods": set(
                [method.value for method in registeredRequestMethod]
            ),
            "tupleClass": TupleClass,
        }

    def getChild(self, path, request):
        """Get requests routed to an handler

        Routes to a registered handler to process the request.
        This function routes resources to handlers which eventually returns
        JSON response resource in json. Everything returned here should be
        final.

        Once url prefix for a resource is matched, it checks the
        request method is allowed. If granted, it delegates the request to the
        registered handler to process the request. The handler should convert
        request to a Tuple and should respond a tuple.

        :param path: route url for a resource, with plugin name prefix stripped
        :param request: twisted.web.http.Request
        :return: twisted.web.resource.Resource
        """
        # route check
        if path not in self._registeredMethods.keys():
            return ErrorJsonResource(404)

        # request method check
        requestMethod = nativeString(request.method)
        if requestMethod not in self._registeredMethods[path]["allowedMethods"]:
            return ErrorJsonResource(403)

        # invoke resource handler
        TupleClass = self._registeredMethods[path]["tupleClass"]

        handler = self._registeredMethods[path]["handler"]

        return _JsonResource(handler, TupleClass)

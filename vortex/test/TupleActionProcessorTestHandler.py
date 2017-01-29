import logging

from twisted.internet import defer
from twisted.internet.defer import Deferred

from vortex.TupleAction import TupleActionABC
from vortex.handler.TupleActionProcessor import TupleActionProcessor, \
    TupleActionProcessorDelegateABC
from vortex.test.PerformTestActionTuple import PerformTestActionTuple

logger = logging.getLogger(__name__)


class TestProcessor(TupleActionProcessorDelegateABC):
    def processTupleAction(self, tupleAction: TupleActionABC) -> Deferred:
        if "FAIL PLEASE" == tupleAction.data:
            raise Exception("Processing failed scenario, exception raised")
        return defer.succeed(True)


actionProcessor = TupleActionProcessor("vortexTestActions")
actionProcessor.addDelegate(PerformTestActionTuple.tupleName(), TestProcessor())

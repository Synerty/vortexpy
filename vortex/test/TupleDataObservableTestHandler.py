import logging
from datetime import datetime

from twisted.internet import task

from vortex.Payload import Payload
from vortex.handler.TupleDataObservableHandler import TuplesProviderABC, \
    TupleDataObservableHandler
from vortex.TupleSelector import TupleSelector
from vortex.test.TestTuple import TestTuple
from vortex.test.TupleDataForTest import makeTestTupleData

logger = logging.getLogger(__name__)
testTuples1Selector = TupleSelector("testTuples1",
                                    {"count": 4})

testTuples2Selector = TupleSelector("testTuples2",
                                    {"count": 7})


class TestTupleProvider(TuplesProviderABC):
    def makeVortexMsg(self, filt: dict, tupleSelector: TupleSelector) -> bytes:
        count = tupleSelector.selector["count"]
        tuples = makeTestTupleData(count)

        for t in tuples:
            t.aDate = datetime.utcnow()
            t.aDict = tupleSelector.selector
            t.aString = tupleSelector.name

        return Payload(filt=filt, tuples=tuples).toVortexMsg()


class NotifyTestTimer:
    @classmethod
    def __notify(cls):
        observableHandler.notifyOfTupleUpdate(testTuples1Selector)
        observableHandler.notifyOfTupleUpdate(testTuples2Selector)



    @classmethod
    def startTupleUpdateNotifyer(cls):
        cls.__loopingCall = task.LoopingCall(cls.__notify)
        d = cls.__loopingCall.start(2)
        d.addErrback(lambda f: logger.exception(f.value))
        d.addCallback(lambda _: logger.debug("Observable tuple updates started"))


observableHandler = TupleDataObservableHandler("vortexTestObservable")
observableHandler.addTupleProvider(TestTuple.tupleName(), TestTupleProvider())
observableHandler.addTupleProvider("testTuples1", TestTupleProvider())
observableHandler.addTupleProvider("testTuples2", TestTupleProvider())
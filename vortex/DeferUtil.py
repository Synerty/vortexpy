import logging

import twisted
from twisted.internet.defer import maybeDeferred
from twisted.internet.threads import deferToThread

logger = logging.getLogger(__name__)

def maybeDeferredWrap(funcToWrap):
    """ Maybe Deferred Wrap

    A decorator that ensures a function will return a deferred.

    """

    def func(*args, **kwargs):
        return maybeDeferred(funcToWrap, *args, **kwargs)

    return func


def vortexLogFailure(failure, loggerArg, consumeError=False, successValue=True):
    try:
        if not hasattr(failure, '_vortexLogged'):
            if failure.getTraceback():
                loggerArg.error(failure.getTraceback())
            failure._vortexFailureLogged = True
        return successValue if consumeError else failure
    except Exception as e:
        logger.exception(e)

printFailure = vortexLogFailure



def deferToThreadWrapWithLogger(logger):
    assert isinstance(logger, logging.Logger), """Usage:
    import logging
    logger = logging.getLogger(__name__)
    @deferToThreadWrapWithLogger(logger)
    def myFunction(arg1, kw=True):
        pass
    """

    def wrapper(funcToWrap):
        def func(*args, **kwargs):
            if not twisted.python.threadable.isInIOThread():
                raise Exception(
                    "Deferring to a thread can only be done from the main thread")

            d = deferToThread(funcToWrap, *args, **kwargs)
            d.addErrback(vortexLogFailure, logger)
            return d

        return func

    return wrapper


def noMainThread():
    if twisted.python.threadable.isInIOThread():
        raise Exception("Blocking operations shouldn't occur in the reactors thread.")

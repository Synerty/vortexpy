import logging

from twisted.internet.threads import deferToThread


def vortexLogFailure(failure, logger, consumeError=False, successValue=True):
    if not hasattr(failure, '_vortexLogged'):
        logger.error(failure.getTraceback())
        failure._vortexFailureLogged = True
    return successValue if consumeError else failure


def printFailure(failure, logger):
    logger.error(failure)
    return failure


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
            d = deferToThread(funcToWrap, *args, **kwargs)
            d.addErrback(vortexLogFailure, logger)
            return d

        return func

    return wrapper

from twisted.internet.threads import deferToThread


def vortexLogFailure(failure, logger):
    if not hasattr(failure, '_vortexLogged'):
        logger.error(failure.getTraceback())
        failure._vortexFailureLogged = True
    return failure


def printFailure(failure, logger):
    logger.error(failure)
    return failure


def deferToThreadWrap(logger):
    def wrapper(funcToWrap):
        def func(*args, **kwargs):
            d = deferToThread(funcToWrap, *args, **kwargs)
            d.addErrback(vortexLogFailure, logger)
            return d

        return func
    return wrapper
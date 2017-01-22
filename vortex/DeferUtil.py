def vortexLogFailure(failure, logger):
    if not hasattr(failure, '_vortexLogged'):
        logger.error(failure.getTraceback())
        failure._vortexFailureLogged = True
    return failure

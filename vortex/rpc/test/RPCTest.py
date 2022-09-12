from vortex.rpc.RPC import vortexRPC


@vortexRPC("listenVortexName", acceptOnlyFromVortex="sendVortexName")
def myRemoteExceptionMethod(arg1: int, kwarg1: int = 0) -> int:
    raise ValueError("A is not B")


@vortexRPC("listenVortexName", acceptOnlyFromVortex="sendVortexName")
def myRemoteAddMethod(arg1: int, kwarg1: int = 0) -> int:
    print(arg1, kwarg1)
    return arg1 + kwarg1

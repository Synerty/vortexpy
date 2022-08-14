from vortex.rpc.RPC import vortexRPC


@vortexRPC("listenVortexName", acceptOnlyFromVortex="sendVortexName")
def myRemoteAddMethod(arg1: int, kwarg1: int = 0) -> int:
    print(arg1, kwarg1)
    raise ValueError("A is not B")

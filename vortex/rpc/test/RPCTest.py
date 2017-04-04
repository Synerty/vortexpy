# import unittest
#
#
# class MyTestCase(unittest.TestCase):
#     def test_something(self):
#         self.assertEqual(True, False)
#
#
# if __name__ == '__main__':
#     unittest.main()
from vortex.rpc.RPC import vortexRPC


@vortexRPC("listenVortexName", acceptOnlyFromVortex="sendVortexName")
def myRemoteAddMethod(arg1: int, kwarg1: int = 0) -> int:
    raise ValueError("A is not B")
    return arg1 + kwarg1

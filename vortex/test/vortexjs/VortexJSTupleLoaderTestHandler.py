# import logging
#
# from vortex.PayloadFilterKeys import plDeleteKey
# from vortex.test.TestTuple import TestTuple
# from vortex.test.TupleDataForTest import makeTestTupleData
#
# logger = logging.getLogger(__name__)
#
#
# # If there were reallying going to a DB, we'd use OrmCrudHandler
# class VortexJSTupleLoaderTestHandler(ModelHandler):
#     def buildModel(self, payload, **kwargs):
#         logger.debug("Received payload with %s tuples and filt=%s",
#                      len(payload.tuples), payload.filt)
#
#         data = []
#
#         if payload.tuples:
#             # Return nothing if this was a delete
#             if plDeleteKey in payload.filt:
#                 # Return nothing, it was deleted
#                 pass
#
#             else:
#                 # Else this was a save, just update some data and return id
#                 data = payload.tuples
#                 for testTuple in data:
#                     testTuple.aInt = (testTuple.aInt if testTuple.aInt else 0) + 10
#                     testTuple.aBoolTrue = not (testTuple.aBoolTrue
#                                                if testTuple.aBoolTrue
#                                                else 0)
#
#         else:
#             # Else this is to get new data.
#             data = makeTestTupleData()
#
#         return data
#
#
# __handler = VortexJSTupleLoaderTestHandler({
#     "key": "vortex.tuple-loader.test.data"
# })

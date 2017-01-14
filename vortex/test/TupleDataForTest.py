"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
from vortex.test.TestTuple import TestTuple


def makeTestTupleData(count=5):
    tuples = []
    for num in range(count):
        uniStr = "#%s double hyphen :-( — “fancy quotes”" % num
        tuples.append(TestTuple(aInt=num,
                              aBoolTrue=bool(num % 2),
                              aString="This is tuple #%s" % num,
                              aStrWithUnicode=uniStr))

    return tuples

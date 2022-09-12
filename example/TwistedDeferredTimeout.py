# from twisted.internet import task, defer
#
# def logTimeout(result, timeout):
#     print("Got {0!r} but actually timed out after {1} seconds".format(
#         result, timeout))
#     return result + " (timed out)"
#
# def main(reactor):
#     # generate a deferred with a custom canceller function, and never
#     # never callback or errback it to guarantee it gets timed out
#     d = defer.Deferred()#lambda c: c.callback("Everything's ok!"))
#     d.addTimeout(2, reactor)#, onTimeoutCancel=logTimeout)
#     d.addBoth(print)
#     return d
#
# task.react(main)
#


import random

from twisted.internet import task


def f():
    return "Hopefully this will be called in 3 seconds or less"


def main(reactor):
    delay = random.uniform(1, 5)

    def called(result):
        print("{0} seconds later:".format(delay), result)

    d = task.deferLater(reactor, delay, f)
    d.addTimeout(3, reactor).addBoth(called)

    return d


# f() will be timed out if the random delay is greater than 3 seconds
task.react(main)

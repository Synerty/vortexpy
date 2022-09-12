import logging
import sys
from datetime import datetime


def setupExampleLogging():
    logging.root.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    logging.root.addHandler(console)

    class MyFormatter(logging.Formatter):
        converter = datetime.fromtimestamp

        def formatTime(self, record, datefmt=None):
            ct = self.converter(record.created)
            if datefmt:
                s = ct.strftime(datefmt)
            else:
                t = ct.strftime("%Y-%m-%d %H:%M:%S")
                s = "%s,%03d" % (t, record.msecs)
            return s

    formatter = MyFormatter(
        fmt="%(asctime)s %(message)s", datefmt="%Y-%m-%d,%H:%M:%S.%f"
    )
    console.setFormatter(formatter)

    # Start the twisted logging

    from twisted.python import log

    log.startLogging(sys.stdout)

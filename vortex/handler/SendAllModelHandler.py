"""
 * Created by Synerty Pty Ltd
 *
 * This software is open source, the MIT license applies.
 *
 * Website : http://www.synerty.com
 * Support : support@synerty.com
"""
import logging

from txhttputil.util.DeferUtil import deferToThreadWrap

from vortex.handler.ModelHandler import ModelHandler

logger = logging.getLogger(__name__)


class SendAllModelHandler(ModelHandler):
    def __init__(self, payloadFilter, OrmClass, ormSessionFunction):
        ModelHandler.__init__(self, payloadFilter)
        self._OrmClass = OrmClass
        self._ormSessionFunction = ormSessionFunction

    @deferToThreadWrap
    def buildModel(self, **kwargs):
        ormSession = self._ormSessionFunction()
        tuples = ormSession.query(self._OrmClass).all()
        ormSession.close()

        return tuples

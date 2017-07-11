import logging
from datetime import datetime
from typing import List

from sqlalchemy import bindparam
from twisted.internet import defer
from twisted.internet.defer import Deferred, inlineCallbacks

from peek_plugin_livedb._private.server.LiveDBReadApi import LiveDBReadApi
from peek_plugin_livedb._private.server.controller.LiveDbController import \
    LiveDbController
from peek_plugin_livedb._private.server.controller.LiveDbImportController import \
    LiveDbImportController
from peek_plugin_livedb._private.storage.LiveDbItem import LiveDbItem
from peek_plugin_livedb._private.storage.LiveDbModelSet import getOrCreateLiveDbModelSet
from peek_plugin_livedb.server.LiveDBWriteApiABC import LiveDBWriteApiABC
from peek_plugin_livedb.tuples.ImportLiveDbItemTuple import ImportLiveDbItemTuple
from peek_plugin_livedb.tuples.LiveDbDisplayValueUpdateTuple import \
    LiveDbDisplayValueUpdateTuple
from peek_plugin_livedb.tuples.LiveDbRawValueUpdateTuple import LiveDbRawValueUpdateTuple
from vortex.DeferUtil import deferToThreadWrapWithLogger

logger = logging.getLogger(__name__)


class LiveDBWriteApi(LiveDBWriteApiABC):
    def __init__(self, liveDbController: LiveDbController,
                 liveDbImportController: LiveDbImportController,
                 readApi: LiveDBReadApi,
                 dbSessionCreator,
                 dbEngine):
        self._liveDbController = liveDbController
        self._liveDbImportController = liveDbImportController
        self._readApi = readApi
        self._dbSessionCreator = dbSessionCreator
        self._dbEngine = dbEngine

    def shutdown(self):
        pass

    @inlineCallbacks
    def updateRawValues(self, modelSetName: str,
                        updates: List[LiveDbRawValueUpdateTuple]) -> Deferred:
        yield updateValues(self._dbEngine, self._dbSessionCreator,
                           modelSetName, updates, raw=True)
        self._readApi.rawValueUpdatesObservable(modelSetName).on_next(updates)

    @inlineCallbacks
    def updateDisplayValue(self, modelSetName: str,
                           updates: List[LiveDbDisplayValueUpdateTuple]) -> Deferred:
        yield updateValues(self._dbEngine, self._dbSessionCreator,
                           modelSetName, updates, raw=False)
        self._readApi.displayValueUpdatesObservable(modelSetName).on_next(updates)

    def importLiveDbItems(self, modelSetName: str,
                          newItems: List[ImportLiveDbItemTuple]) -> Deferred:
        return self._liveDbImportController.importDispLiveDbItems(modelSetName, newItems)

    def prioritiseLiveDbValueAcquisition(self, modelSetName: str,
                                         liveDbKeys: List[str]) -> Deferred:
        self._readApi.priorityLiveDbKeysObservable(modelSetName).on_next(liveDbKeys)
        return defer.succeed(True)


@deferToThreadWrapWithLogger(logger)
def updateValues(dbEngine, dbSessionCreator, modelSetName, updates, raw=True):
    startTime = datetime.utcnow()
    table = LiveDbItem.__table__

    session = dbSessionCreator()
    conn = dbEngine.connect()
    try:
        liveDbModelSet = getOrCreateLiveDbModelSet(session, modelSetName)

        sql = (table.update()
               .where(table.c.key == bindparam('key'),
                      table.c.modelSetId == liveDbModelSet.id)
               .values({"rawValue" if raw else "displayValue": bindparam("value")}))

        conn.execute(sql, [
            dict(key=o.key, value=(o.rawValue if raw else o.displayValue))
            for o in updates])

        logger.debug("Updated %s %s values, in %s",
                     len(updates),
                     "raw" if raw else "display",
                     (datetime.utcnow() - startTime))

    except Exception as e:
        logger.critical(e)

    finally:
        session.close()
        conn.close()

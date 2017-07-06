from typing import List

import logging
from twisted.internet import defer
from twisted.internet.defer import Deferred

from peek_plugin_livedb._private.server.controller.LiveDbController import \
    LiveDbController
from peek_plugin_livedb._private.server.controller.LiveDbImportController import \
    LiveDbImportController
from peek_plugin_livedb.server.LiveDBWriteApiABC import LiveDBWriteApiABC
from peek_plugin_livedb.tuples.ImportLiveDbItemTuple import ImportLiveDbItemTuple
from peek_plugin_livedb.tuples.LiveDbDisplayValueTuple import LiveDbDisplayValueTuple
from peek_plugin_livedb.tuples.LiveDbRawValueTuple import LiveDbRawValueTuple

logger = logging.getLogger(__name__)

class LiveDBWriteApi(LiveDBWriteApiABC):
    def __init__(self, liveDbController: LiveDbController,
                 liveDbImportController: LiveDbImportController):
        self._liveDbController = liveDbController
        self._liveDbImportController = liveDbImportController

    def shutdown(self):
        pass

    def processLiveDbRawValueUpdates(self, modelSetName: str,
                                     updates: LiveDbRawValueTuple) -> Deferred:
        return defer.succeed(True)

    def processLiveDbDisplayValueUpdates(self, modelSetName: str,
                                         updates: LiveDbDisplayValueTuple) -> Deferred:
        return defer.succeed(True)

    def importLiveDbItems(self, modelSetName: str,
                          newItems: ImportLiveDbItemTuple) -> Deferred:
        logger.debug("importLiveDbItems called for %s items (TODO)" % len(newItems))
        return defer.succeed(True)

    def prioritiseLiveDbValueAcquisition(self, modelSetName: str,
                                         liveDbKeys: List[str]) -> Deferred:
        return defer.succeed(True)

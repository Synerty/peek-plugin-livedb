from rx.subjects import Subject
from twisted.internet import defer
from twisted.internet.defer import Deferred

from peek_plugin_livedb._private.server.controller.LiveDbController import \
    LiveDbController
from peek_plugin_livedb._private.server.controller.LiveDbImportController import \
    LiveDbImportController
from peek_plugin_livedb.server.LiveDBReadApiABC import LiveDBReadApiABC


class LiveDBReadApi(LiveDBReadApiABC):

    def __init__(self, liveDbController: LiveDbController,
                 liveDbImportController: LiveDbImportController):
        self._liveDbController = liveDbController
        self._liveDbImportController = liveDbImportController

    def shutdown(self):
        pass

    def monitorLiveDbIdsObservable(self, modelSetName: str) -> Subject:
        return Subject()

    def liveDbTupleAdditionsObservable(self, modelSetName: str) -> Deferred:
        return defer.succeed(True)

    def liveDbTupleRemovalsObservable(self, modelSetName: str) -> Deferred:
        return defer.succeed(True)

    def liveDbTuplesDeferredGenerator(self, modelSetName: str) -> Deferred:
        return defer.succeed(True)

    def liveDbValueUpdatesObservable(self, modelSetName: str) -> Subject:
        return Subject()
import logging
from typing import List

from collections import defaultdict
from rx.subjects import Subject
from twisted.internet import defer
from twisted.internet.defer import Deferred

from peek_plugin_livedb._private.server.controller.LiveDbController import \
    LiveDbController
from peek_plugin_livedb._private.storage.LiveDbItem import LiveDbItem
from peek_plugin_livedb.server.LiveDBReadApiABC import LiveDBReadApiABC
from peek_plugin_livedb.tuples.LiveDbDisplayValueTuple import LiveDbDisplayValueTuple
from vortex.DeferUtil import deferToThreadWrapWithLogger

logger = logging.getLogger(__name__)


class LiveDBReadApi(LiveDBReadApiABC):
    def __init__(self, liveDbController: LiveDbController,
                 dbSessionCreator,
                 dbEngine):
        self._liveDbController = liveDbController
        self._dbSessionCreator = dbSessionCreator
        self._dbEngine = dbEngine

        self._prioritySubject = defaultdict(Subject)
        self._additionsSubject = defaultdict(Subject)
        self._deletionsSubject = defaultdict(Subject)
        self._rawValueUpdatesSubject = defaultdict(Subject)
        self._displayValueUpdatesSubject = defaultdict(Subject)

    def shutdown(self):
        pass

    def priorityLiveDbKeysObservable(self, modelSetName: str) -> Subject:
        return self._prioritySubject[modelSetName]

    def itemAdditionsObservable(self, modelSetName: str) -> Subject:
        return self._additionsSubject[modelSetName]

    def itemDeletionsObservable(self, modelSetName: str) -> Subject:
        return self._deletionsSubject[modelSetName]

    def bulkLoadDeferredGenerator(self, modelSetName: str) -> Deferred:
        offset = 0
        limit = 10000
        while True:
            yield qryChunk(offset, limit, self._dbSessionCreator)
            offset += limit


    def rawValueUpdatesObservable(self, modelSetName: str) -> Subject:
        return self._rawValueUpdatesSubject[modelSetName]

    def displayValueUpdatesObservable(self, modelSetName: str) -> Subject:
        return self._displayValueUpdatesSubject[modelSetName]


@deferToThreadWrapWithLogger(logger)
def qryChunk(offset, limit, dbSessionCreator) -> List[LiveDbDisplayValueTuple]:
    session = dbSessionCreator()
    try:
        qry = (session.query(LiveDbItem)
               .order_by(LiveDbItem.id)
               .offset(offset)
               .limit(limit)
               .yield_per(10000))

        return [LiveDbDisplayValueTuple(
            key=o.key, dataType=o.dataType,
            rawValue=o.rawValue, displayValue=o.displayValue) for o in qry]

    finally:
        session.close()

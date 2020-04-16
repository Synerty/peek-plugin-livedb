import logging
from datetime import datetime
from typing import List

import pytz
from sqlalchemy.sql.expression import asc
from twisted.internet import task, reactor, defer
from twisted.internet.defer import inlineCallbacks
from vortex.DeferUtil import deferToThreadWrapWithLogger, vortexLogFailure

from peek_plugin_livedb._private.server.controller.AdminStatusController import \
    AdminStatusController
from peek_plugin_livedb._private.storage.LiveDbRawValueQueue import LiveDbRawValueQueue
from peek_plugin_livedb.tuples.LiveDbRawValueUpdateTuple import LiveDbRawValueUpdateTuple

logger = logging.getLogger(__name__)


class LiveDbRawValueUpdateQueueController:
    """ Grid Compiler

    Compile the disp items into the grid data

    1) Query for queue
    2) Process queue
    3) Delete from queue
    """

    ITEMS_PER_TASK = 500
    PERIOD = 0.200

    QUEUE_MAX = 20
    QUEUE_MIN = 4

    TASK_TIMEOUT = 60.0

    def __init__(self, ormSessionCreator, adminStatusController: AdminStatusController):
        self._dbSessionCreator = ormSessionCreator
        self._adminStatusController: AdminStatusController = adminStatusController

        self._pollLoopingCall = task.LoopingCall(self._poll)
        self._lastQueueId = 0
        self._queueCount = 0

        self._chunksInProgress = set()

    def isBusy(self) -> bool:
        return self._queueCount != 0

    def start(self):
        self._adminStatusController.setRawValueStatus(True, self._queueCount)
        d = self._pollLoopingCall.start(self.PERIOD, now=False)
        d.addCallbacks(self._timerCallback, self._timerErrback)

    def _timerErrback(self, failure):
        vortexLogFailure(failure, logger)
        self._adminStatusController.setRawValueStatus(False, self._queueCount)
        self._adminStatusController.setRawValueError(str(failure.value))

    def _timerCallback(self, _):
        self._adminStatusController.setRawValueStatus(False, self._queueCount)

    def stop(self):
        if self._pollLoopingCall.running:
            self._pollLoopingCall.stop()

    def shutdown(self):
        self.stop()

    @inlineCallbacks
    def _poll(self):
        # We queue the grids in bursts, reducing the work we have to do.
        if self._queueCount > self.QUEUE_MIN:
            return

        queueItems = yield self._grabQueueChunk()

        if not queueItems:
            return

        for start in range(0, len(queueItems), self.ITEMS_PER_TASK):

            items = queueItems[start: start + self.ITEMS_PER_TASK]

            # If we're already processing these chunks, then return and try later
            if self._chunksInProgress & set([o.modelSetKeyKey for o in items]):
                return

            # This should never fail
            d = self._sendToWorker(items)
            d.addErrback(vortexLogFailure, logger)

            # Set the watermark
            self._lastQueueId = items[-1].id

            self._queueCount += 1
            if self._queueCount >= self.QUEUE_MAX:
                break

        self._adminStatusController.setRawValueStatus(True, self._queueCount)

    @inlineCallbacks
    def _sendToWorker(self, items: List[LiveDbRawValueQueue]):
        from peek_plugin_livedb._private.worker.tasks.LiveDbItemUpdateTask import \
            updateValues

        queueIds = [o.id for o in items]
        lastestUpdates = {}
        for item in items:
            lastestUpdates[item.modelSetKeyKey] = item

        lastestUpdates = list(lastestUpdates.values())

        startTime = datetime.now(pytz.utc)

        # Add the chunks we're processing to the set
        self._chunksInProgress |= set([o.modelSetKeyKey for o in items])

        try:
            d = updateValues.delay(queueIds, lastestUpdates)
            d.addTimeout(self.TASK_TIMEOUT, reactor)

            yield d
            logger.debug("%s Raw Values, Time Taken = %s",
                         len(items), datetime.now(pytz.utc) - startTime)

            self._queueCount -= 1

            self._adminStatusController.setRawValueStatus(True, self._queueCount)
            self._adminStatusController.addToRawValueTotal(self.ITEMS_PER_TASK)

            # Success, Remove the chunks from the in-progress queue
            self._chunksInProgress -= set([o.modelSetKeyKey for o in items])

        except Exception as e:
            if isinstance(e, defer.TimeoutError):
                logger.info("Retrying compile, Task has timed out.")
            else:
                logger.debug("Retrying compile : %s", str(e))

            reactor.callLater(2.0, self._sendToWorker, items)
            return

    @deferToThreadWrapWithLogger(logger)
    def _grabQueueChunk(self):
        toGrab = (self.QUEUE_MAX - self._queueCount) * self.ITEMS_PER_TASK
        session = self._dbSessionCreator()
        try:
            queueItems = (session.query(LiveDbRawValueQueue)
                          .order_by(asc(LiveDbRawValueQueue.id))
                          .filter(LiveDbRawValueQueue.id > self._lastQueueId)
                          .yield_per(toGrab)
                          .limit(toGrab)
                          .all())

            session.expunge_all()
            return queueItems
        finally:
            session.close()

    @deferToThreadWrapWithLogger(logger)
    def queueData(self, modelSetKey: str,
                  updates: List[LiveDbRawValueUpdateTuple]):
        return self.queueDataToCompile(modelSetKey, updates, self._dbSessionCreator)

    @classmethod
    def queueDataToCompile(cls, modelSetKey: str,
                           updates: List[LiveDbRawValueUpdateTuple],
                           ormSessionCreator):
        if not updates:
            return

        ormSession = ormSessionCreator()
        try:
            cls.queueDataToCompileWithSession(modelSetKey, updates, ormSession)
            ormSession.commit()

        finally:
            ormSession.close()

    @staticmethod
    def queueDataToCompileWithSession(modelSetKey: str,
                                      updates: List[LiveDbRawValueUpdateTuple],
                                      ormSessionOrConn):
        if not updates:
            return

        logger.debug("Queueing %s raw values for compile", len(updates))

        inserts = []
        for update in updates:
            inserts.append(dict(modelSetKey=modelSetKey,
                                key=update.key,
                                rawValue=update.rawValue))

        ormSessionOrConn.execute(LiveDbRawValueQueue.__table__.insert(), inserts)

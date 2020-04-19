import logging
from collections import deque, namedtuple
from datetime import datetime
from typing import List, Deque

import pytz
from sqlalchemy.sql.expression import asc, select, bindparam
from twisted.internet import task, reactor, defer
from twisted.internet.defer import inlineCallbacks
from vortex.DeferUtil import deferToThreadWrapWithLogger, vortexLogFailure

from peek_plugin_livedb._private.server.controller.AdminStatusController import \
    AdminStatusController
from peek_plugin_livedb._private.storage.LiveDbModelSet import getOrCreateLiveDbModelSet
from peek_plugin_livedb._private.storage.LiveDbRawValueQueue import LiveDbRawValueQueue, \
    LiveDbRawValueQueueTuple
from peek_plugin_livedb.tuples.LiveDbRawValueUpdateTuple import LiveDbRawValueUpdateTuple

logger = logging.getLogger(__name__)

_BlockItem = namedtuple("_QueueItem", ("queueIds", "items", "itemUniqueIds"))


class LiveDbRawValueUpdateQueueController:
    """ Grid Compiler

    Compile the disp items into the grid data

    1) Query for queue
    2) Process queue
    3) Delete from queue
    """

    ITEMS_PER_TASK = 500
    PERIOD = 0.200

    # We don't deduplicate this queue, so we can fill it up
    QUEUE_MAX = 40
    QUEUE_MIN = 8

    TASK_TIMEOUT = 60.0

    def __init__(self, ormSessionCreator, adminStatusController: AdminStatusController):
        self._dbSessionCreator = ormSessionCreator
        self._statusController: AdminStatusController = adminStatusController

        self._pollLoopingCall = task.LoopingCall(self._poll)
        self._queueCount = 0

        self._queueIdsInBuffer = set()
        self._chunksInProgress = set()

        self._pausedForDuplicate = None
        self._fetchedBlockBuffer: Deque[_BlockItem] = deque()

    def isBusy(self) -> bool:
        return self._queueCount != 0

    def start(self):
        self._statusController.setRawValueStatus(True, self._queueCount)
        d = self._pollLoopingCall.start(self.PERIOD, now=False)
        d.addCallbacks(self._timerCallback, self._timerErrback)

    def _timerErrback(self, failure):
        vortexLogFailure(failure, logger)
        self._statusController.setRawValueStatus(False, self._queueCount)
        self._statusController.setRawValueError(str(failure.value))

    def _timerCallback(self, _):
        self._statusController.setRawValueStatus(False, self._queueCount)

    def stop(self):
        if self._pollLoopingCall.running:
            self._pollLoopingCall.stop()

    def shutdown(self):
        self.stop()

    @inlineCallbacks
    def _poll(self):
        # If the Queue compiler is paused, then do nothing.
        if self._pausedForDuplicate:
            return

        # We queue the grids in bursts, reducing the work we have to do.
        if self._queueCount > self.QUEUE_MIN:
            return

        fetchedBlocks = yield self._fetchBlocks()
        # Queue the next blocks
        self._fetchedBlockBuffer.extend(fetchedBlocks)

        # If we have nothing to do, exit now
        if not self._fetchedBlockBuffer:
            return

        # Process the block buffer
        while self._fetchedBlockBuffer:
            # Look at the next block to process
            block = self._fetchedBlockBuffer[0]

            # If we're already processing these chunks, then return and try later
            if self._chunksInProgress & block.itemUniqueIds:
                self._pausedForDuplicate = block.itemUniqueIds
                return

            # We're going to process it, remove it from the buffer
            self._fetchedBlockBuffer.popleft()

            # This should never fail
            d = self._sendToWorker(block)
            d.addErrback(vortexLogFailure, logger)

            self._queueCount += 1
            if self._queueCount >= self.QUEUE_MAX:
                break

        self._statusController.setRawValueStatus(True, self._queueCount)
        yield self._dedupeQueue()

    @inlineCallbacks
    def _sendToWorker(self, block: _BlockItem):
        from peek_plugin_livedb._private.worker.tasks.LiveDbItemUpdateTask import \
            updateValues

        startTime = datetime.now(pytz.utc)

        # Add the chunks we're processing to the set
        self._chunksInProgress |= block.itemUniqueIds

        try:
            d = updateValues.delay(block.queueIds, block.items)
            d.addTimeout(self.TASK_TIMEOUT, reactor)

            yield d
            logger.debug("%s items, Time Taken = %s",
                         len(block.items), datetime.now(pytz.utc) - startTime)

            # Success, Remove the chunks from the in-progress queue
            self._queueCount -= 1
            self._chunksInProgress -= block.itemUniqueIds
            self._queueIdsInBuffer -= set(block.queueIds)

            # If the queue compiler was paused for this chunk then resume it.
            if self._pausedForDuplicate and self._pausedForDuplicate & block.itemUniqueIds:
                self._pausedForDuplicate = None

            # Notify the status controller
            self._statusController.setRawValueStatus(True, self._queueCount)
            self._statusController.addToRawValueTotal(len(block.items))

        except Exception as e:
            if isinstance(e, defer.TimeoutError):
                logger.info("Retrying compile, Task has timed out.")
            else:
                logger.debug("Retrying compile : %s", str(e))

            reactor.callLater(2.0, self._sendToWorker, block)
            return

    @deferToThreadWrapWithLogger(logger)
    def _fetchBlocks(self) -> List[_BlockItem]:
        queueTable = LiveDbRawValueQueue.__table__

        toGrab = self.QUEUE_MAX - self._queueCount
        toGrab *= self.ITEMS_PER_TASK

        session = self._dbSessionCreator()
        try:
            sql = select([queueTable]) \
                .order_by(asc(queueTable.c.id)) \
                .limit(bindparam('b_toGrab'))

            sqlData = session \
                .execute(sql, dict(b_toGrab=toGrab)) \
                .fetchall()

            queueItems = [LiveDbRawValueQueueTuple(o.id, o.modelSetId, o.key, o.rawValue)
                          for o in sqlData
                          if o.id not in self._queueIdsInBuffer]

            queueBlocks = []
            for start in range(0, len(queueItems), self.ITEMS_PER_TASK):

                queueIds = []
                lastestUpdates = {}
                for item in queueItems[start: start + self.ITEMS_PER_TASK]:
                    queueIds.append(item.id)
                    lastestUpdates[item.modelSetIdKey] = item

                itemUniqueIds = set(lastestUpdates.keys())
                items = list(lastestUpdates.values())

                self._queueIdsInBuffer.update(queueIds)

                queueBlocks.append(_BlockItem(queueIds, items, itemUniqueIds))

            return queueBlocks

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

        modelSet = getOrCreateLiveDbModelSet(ormSessionOrConn, modelSetKey=modelSetKey)

        inserts = []
        for update in updates:
            inserts.append(dict(modelSetId=modelSet.id,
                                key=update.key,
                                rawValue=update.rawValue))

        ormSessionOrConn.execute(LiveDbRawValueQueue.__table__.insert(), inserts)

    # @deferToThreadWrapWithLogger(logger)
    def _dedupeQueue(self):
        """ We can't reliably dedupe the livedb queue.

        This is because we delete the later entries, and the later entries have the
        latest raw value.

        We have to delete the later entries, otherwise if we get lots of updates,
        for the same item if we delete an early one, consume some rows, then dedupe
        again, the value just keeps getting moved further and further down
        the queue.

        """
        # session = self._dbSessionCreator()
        # dedupeLimit = self.QUEUE_MAX * self.ITEMS_PER_TASK * 2
        # try:
        #     sql = """
        #          with sq_raw as (
        #             SELECT "id", "modelSetId", "key"
        #             FROM livedb."LiveDbRawValueQueue"
        #             WHERE id > %(id)s
        #             LIMIT %(limit)s
        #         ), sq as (
        #             SELECT max(id) as "maxId", "modelSetId", "key"
        #             FROM sq_raw
        #             GROUP BY "modelSetId", "key"
        #             HAVING count("modelSetId") > 1
        #         )
        #         DELETE
        #         FROM pl_diagram."LiveDbRawValueQueue"
        #              USING sq sq1
        #         WHERE "id" != sq1."maxId"
        #             AND "id" > %(id)s
        #             AND "modelSetId" = sq1."modelSetId"
        #             AND "key" = sq1."key"
        #
        #     """ % {'id': self._lastFetchedId, 'limit': dedupeLimit}
        #
        #     session.execute(sql)
        #     session.commit()
        # finally:
        #     session.close()

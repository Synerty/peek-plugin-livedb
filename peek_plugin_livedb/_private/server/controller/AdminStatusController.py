import logging
from datetime import datetime

import pytz
from twisted.internet import reactor
from vortex.TupleSelector import TupleSelector
from vortex.handler.TupleDataObservableHandler import TupleDataObservableHandler

from peek_plugin_livedb._private.tuples.AdminStatusTuple import \
    AdminStatusTuple

logger = logging.getLogger(__name__)


class AdminStatusController:
    NOTIFY_PERIOD = 2.0

    def __init__(self):
        self._tupleObserver = None
        self._status = AdminStatusTuple()
        self._notifyPending = False
        self._lastNotifyDatetime = datetime.now(pytz.utc)

    def setTupleObservable(self, tupleObserver: TupleDataObservableHandler):
        self._tupleObserver = tupleObserver

    def shutdown(self):
        self._tupleObserver = None

    @property
    def status(self):
        return self._status

    # ---------------
    # Display Compiler Methods

    def setRawValueStatus(self, state: bool, queueSize: int):
        self._status.rawValueQueueStatus = state
        self._status.rawValueQueueSize = queueSize
        self._notify()

    def addToRawValueTotal(self, delta: int):
        self._status.rawValueProcessedTotal += delta
        self._notify()

    def setRawValueError(self, error: str):
        self._status.rawValueLastError = error
        self._notify()

    # ---------------
    # Notify Methods

    def _notify(self):
        if self._notifyPending:
            return

        self._notifyPending = True

        deltaSeconds = (datetime.now(pytz.utc) - self._lastNotifyDatetime).seconds
        if deltaSeconds >= self.NOTIFY_PERIOD:
            self._sendNotify()
        else:
            reactor.callLater(self.NOTIFY_PERIOD - deltaSeconds, self._sendNotify)

    def _sendNotify(self):
        self._notifyPending = False
        self._lastNotifyDatetime = datetime.now(pytz.utc)
        self._tupleObserver.notifyOfTupleUpdate(
            TupleSelector(AdminStatusTuple.tupleType(), {})
        )

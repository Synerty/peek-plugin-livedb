import logging
from datetime import datetime

from sqlalchemy import select
from txcelery.defer import CeleryClient

from peek_plugin_base.worker import CeleryDbConn
from peek_plugin_livedb._private.storage.LiveDbItem import LiveDbItem
from peek_plugin_livedb._private.worker.CeleryApp import celeryApp
from peek_plugin_livedb.tuples.LiveDbDisplayValueTuple import LiveDbDisplayValueTuple
from vortex.Payload import Payload

logger = logging.getLogger(__name__)


@CeleryClient
@celeryApp.task(bind=True)
def qryChunkInWorker(self, offset, limit) -> str:
    """ Query Chunk

    This returns a chunk of LiveDB items from the database

    :param self: A celery reference to this task
    :param offset: The offset of the chunk
    :param limit: An encoded payload containing the updates
    :returns: List[LiveDbDisplayValueTuple] serialised in a payload json
    """

    table = LiveDbItem.__table__
    cols = [table.c.key, table.c.dataType, table.c.rawValue, table.c.displayValue]

    session = CeleryDbConn.getDbSession()
    try:
        result = session.execute(select(cols)
                                 .order_by(table.c.id)
                                 .offset(offset)
                                 .limit(limit))

        tuples = [LiveDbDisplayValueTuple(
            key=o.key, dataType=o.dataType,
            rawValue=o.rawValue, displayValue=o.displayValue) for o in result.fetchall()]

        return Payload(tuples=tuples)._toJson()

    finally:
        session.close()

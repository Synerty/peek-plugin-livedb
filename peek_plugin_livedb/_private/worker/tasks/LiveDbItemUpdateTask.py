import logging
from datetime import datetime

from collections import namedtuple
from sqlalchemy.sql.expression import bindparam, and_
from txcelery.defer import CeleryClient, DeferrableTask

from peek_plugin_base.worker import CeleryDbConn
from peek_plugin_livedb._private.storage.LiveDbItem import LiveDbItem
from peek_plugin_livedb._private.storage.LiveDbModelSet import getOrCreateLiveDbModelSet
from peek_plugin_livedb._private.worker.CeleryApp import celeryApp
from vortex.Payload import Payload

logger = logging.getLogger(__name__)


@DeferrableTask
@celeryApp.task(bind=True)
def updateValues(self, modelSetName, updatesPayload, raw=True):
    """ Compile Grids Task

    :param self: A celery reference to this task
    :param modelSetName: The model set name
    :param updatesPayload: An encoded payload containing the updates
    :param raw: Are the updates raw updates?
    :returns: A list of grid keys that have been updated.
    """
    updates = Payload()._fromJson(updatesPayload).tuples

    startTime = datetime.utcnow()
    table = LiveDbItem.__table__

    session = CeleryDbConn.getDbSession()
    conn = CeleryDbConn.getDbEngine().connect()
    try:
        liveDbModelSet = getOrCreateLiveDbModelSet(session, modelSetName)

        sql = (table.update()
               .where(and_(table.c.key == bindparam('b_key'),
                           table.c.modelSetId == liveDbModelSet.id))
               .values({"rawValue" if raw else "displayValue": bindparam("b_value")}))

        conn.execute(sql, [
            dict(b_key=o.key, b_value=(o.rawValue if raw else o.displayValue))
            for o in updates])

        logger.debug("Updated %s %s values, in %s",
                     len(updates),
                     "raw" if raw else "display",
                     (datetime.utcnow() - startTime))

    except Exception as e:
        logger.exception(e)
        raise self.retry(exc=e, countdown=2)

    finally:
        session.close()
        conn.close()

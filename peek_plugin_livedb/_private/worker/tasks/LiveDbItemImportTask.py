import logging
import zlib
from _collections import defaultdict
from datetime import datetime
from typing import List

from collections import namedtuple
from functools import cmp_to_key
from sqlalchemy.sql.expression import cast, select
from sqlalchemy.sql.sqltypes import String
from txcelery.defer import CeleryClient

from peek_plugin_base.worker import CeleryDbConn
from peek_plugin_livedb._private.storage.LiveDbItem import LiveDbItem
from peek_plugin_livedb._private.storage.LiveDbModelSet import getOrCreateLiveDbModelSet
from peek_plugin_livedb._private.worker.CeleryApp import celeryApp
from peek_plugin_livedb.tuples.ImportLiveDbItemTuple import ImportLiveDbItemTuple
from vortex.Payload import Payload

logger = logging.getLogger(__name__)

DispData = namedtuple('DispData', ['json', 'id', 'levelOrder', 'layerOrder'])


class LiveDbItemImportTask:
    """ Live DB Item Import Task

    Compile the disp items into the grid data

    1) Query for existing items
    2) Insert new items
    2) Return a list of keys that were inserted
    """

    def import_(self, modelSetName: str,
                    newItems: List[ImportLiveDbItemTuple]) -> List[str]:

        startTime = datetime.utcnow()

        session = CeleryDbConn.getDbSession()
        conn = CeleryDbConn.getDbEngine().connect()
        transaction = conn.begin()

        liveDbTable = LiveDbItem.__table__
        try:

            liveDbModelSet = getOrCreateLiveDbModelSet(session, modelSetName)

            # This will remove duplicates
            itemsByKey = {i.key: i for i in newItems}

            allKeys = list(itemsByKey)
            existingKeys = set()

            # Query for existing keys, in 1000 chinks
            chunkSize = 1000
            offset = 0
            while True:
                chunk = allKeys[offset:chunkSize]
                if not chunk:
                    break
                offset += chunkSize
                existingKeys.update(conn.execute(select(liveDbTable.c.key)
                                                 .yield_per(chunk)
                                                 .where(liveDbTable.c.key.in_(chunk))
                                                 .as_scalar()))

            inserts = []
            newKeys = []

            for newItem in itemsByKey.values():
                if newItem.key in existingKeys:
                    continue

                inserts.append(dict(
                    modelSetId=liveDbModelSet.id,
                    key=newItem.key,
                    dataType=newItem.dataType,
                    rawValue=newItem.rawValue,
                    displayValue=newItem.displayValue,
                    importHash=newItem.importHash
                ))

                newKeys.append(newItem.key)

            if not inserts:
                return []

            conn.execute(LiveDbItem.__table__.insert(), inserts)

            transaction.commit()
            logger.debug("Inserted %s LiveDbItems, %s already existed, in %s",
                         len(inserts), len(existingKeys), (datetime.utcnow() - startTime))

            return newKeys

        except Exception as e:
            transaction.rollback()
            logger.critical(e)

        finally:
            conn.close()
            session.close()


liveDbItemImportTask = LiveDbItemImportTask()


@CeleryClient
@celeryApp.task(bind=True)
def importLiveDbItems(self, modelSetName: str, newItemsPayloadJson: str) -> List[str]:
    """ Compile Grids Task

    :param self: A celery reference to this task
    :param modelSetName: The model set name
    :param newItemsPayloadJson: An encoded payload containing the new LiveDb items.
    :returns: A list of grid keys that have been updated.
    """
    newItems: List[ImportLiveDbItemTuple] = Payload()._fromJson(
        newItemsPayloadJson).tuples
    try:
        return liveDbItemImportTask.import_(modelSetName, newItems)
    except Exception as e:
        raise self.retry(exc=e, countdown=10)

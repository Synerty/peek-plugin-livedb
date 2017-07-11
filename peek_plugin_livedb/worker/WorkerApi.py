from typing import List

from peek_plugin_livedb._private.storage.LiveDbItem import LiveDbItem
from peek_plugin_livedb._private.storage.LiveDbModelSet import LiveDbModelSet
from peek_plugin_livedb.tuples.LiveDbDisplayValueTuple import LiveDbDisplayValueTuple
from peek_plugin_livedb.tuples.LiveDbRawValueTuple import LiveDbRawValueTuple


class WorkerApi:
    """ Worker Api

    This class allows other classes to work with the LiveDB plugin on the
    worker service.

    """
    _FETCH_SIZE = 5000

    @classmethod
    def getLiveDbDisplayValues(cls,
                               ormSession,
                               modelSetName: str,
                               liveDbKeys: List[str]
                               ) -> List[LiveDbRawValueTuple]:
        """ Get Live DB Display Values

        Return an array of items representing the display values from the LiveDB.

        :param ormSession: The SQLAlchemy orm session from the calling code.
        :param modelSetName: The name of the model set to get the keys for
        :param liveDbKeys: An array of LiveDb Keys.

        :returns: An array of tuples.
        """
        if not liveDbKeys:
            return []

        liveDbKeys = set(liveDbKeys)  # Remove duplicates if any exist.
        qry = (
            ormSession.query(LiveDbItem)
                .join(LiveDbModelSet, LiveDbModelSet.id == LiveDbItem.modelSetId)
                .filter(LiveDbModelSet.name == modelSetName)
                .filter(LiveDbItem.key.in_(liveDbKeys))
                .yield_per(cls._FETCH_SIZE)
        )

        results = []

        for item in qry:
            results.append(
                LiveDbDisplayValueTuple(key=item.key,
                                        displayValue=item.displayValue,
                                        rawValue=item.rawValue,
                                        dataType=item.dataType)
            )

        return results

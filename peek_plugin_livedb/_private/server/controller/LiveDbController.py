import logging
from datetime import datetime

from peek_plugin_livedb._private.storage.LiveDbItem import LiveDbItem
from vortex.DeferUtil import deferToThreadWrapWithLogger

logger = logging.getLogger(__name__)


class LiveDbStatus(object):
    def __init__(self):
        self.agentStatus = "No agent connected"
        self.downloadStatus = "Not started"
        self.monitorStatus = "Not started"

    @property
    def statusNameValues(self):
        return [("Agent Connection Status", self.agentStatus),
                ("Agent LiveDB Download Status", self.downloadStatus),
                ("Agent LiveDB Monitor Status", self.monitorStatus),
                ]


class LiveDbController(object):
    # Singleton

    AGENT_KEY_SEND_CHUNK = 5000

    def __init__(self, dbSessionCreator):
        self._dbSessionCreator = dbSessionCreator

        # self._agents = []
        self._status = LiveDbStatus()

    def shutdown(self):
        pass

    @property
    def statusNameValues(self):
        ip = "TODO" # vortexClientIpPort(self._pofAgentVortexUuid)
        self._status.agentStatus = ("Connected from %s" % ip
                                    if ip else
                                    "No Agent Connected")

        return self._status.statusNameValues

    def registerNewLiveDbKeys(self, keyIds):
        if not keyIds:
            return

        self._sendKeysToAgents(keyIds)

    #==========================================================================
    # Begin the code for the LiveDB download
    def liveDbTuplesDefferedGenerator(self, keyIds=None):

        offset = 0
        while True:
            yield self._yieldKeyChunks(keyIds, offset)
            offset += self.AGENT_KEY_SEND_CHUNK


    @deferToThreadWrapWithLogger(logger)
    def _yieldKeyChunks(self, keyIds, offset):
        if keyIds == []:
            return

        ormSession = self._dbSessionCreator()
        try:
            qry = (ormSession.query(LiveDbItem)
                   .order_by(LiveDbItem.id)
                   .yield_per(self.AGENT_KEY_SEND_CHUNK))

            # you can't have filter limit/offset at the same time
            if keyIds is not None:
                qry = qry.filter(LiveDbItem.id.in_(keyIds))

            self._status.downloadStatus = "Sending chunk %s to %s" % (
                offset, self.AGENT_KEY_SEND_CHUNK + offset)

            data =  qry.offset(offset).limit(self.AGENT_KEY_SEND_CHUNK).all()

            if not data:
                self._status.downloadStatus = "Download complete"

            ormSession.expunge_all()

            return data

        finally:
            ormSession.close()

    @deferToThreadWrapWithLogger(logger)
    def processValueUpdates(self, liveDbKeysJson):
        if not liveDbKeysJson:
            return

        startTime = datetime.utcnow()

        engine = SynSqlaConn.dbEngine
        conn = engine.connect()

        session = getNovaOrmSession()

        from peek.core.data_cache.DispLookupDataCache import dispLookupDataCache

        # FIXME, This won't work for multiple model sets
        defaultCoordSet = session.query(ModelCoordSet).first()
        if not defaultCoordSet:
            logger.error("Agent has sent keys that are no longer valid,"
                         " no coord sets")
            return

        defaultCoordSetId = defaultCoordSet.id
        valueTranslator = dispLookupDataCache.getHandler(defaultCoordSetId)

        liveDbKeyIds = set([v['id'] for v in liveDbKeysJson])

        qry = (session.query(LiveDbDispLink.dispId)
               .filter(LiveDbDispLink.liveDbKeyId.in_(liveDbKeyIds))
               .all())

        dispIdsToCompile = list(set([i[0] for i in qry]))

        session.close()

        logger.debug("Queried for %s dispIds in %s",
                     len(dispIdsToCompile), (datetime.utcnow() - startTime))

        transaction = conn.begin()

        # Map to the disp values, that way we can figure out which lookup to use
        for liveDbKey in liveDbKeysJson:
            liveDbKeyId = liveDbKey['id']

            value = liveDbKey['v']
            dataType = liveDbKey['dt']
            convertedValue = valueTranslator.liveDbValueTranslate(dataType, value)

            conn.execute(LiveDbKey.__table__.update()
                         .where(LiveDbKey.id == liveDbKeyId)
                         .values(value=value, convertedValue=convertedValue))

        self._dispCompilerQueue.queueDisps(dispIdsToCompile, conn)

        try:
            transaction.commit()
            logger.debug("Applied %s updates, queued %s disps, from agent in %s",
                         len(liveDbKeysJson),
                         len(dispIdsToCompile),
                         (datetime.utcnow() - startTime))

        except Exception as e:
            transaction.rollback()
            logger.critical(e)

        conn.close()

    def liveDbTuples(self, modelSetName):
        pass

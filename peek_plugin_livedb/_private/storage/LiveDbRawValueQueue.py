import logging
from typing import Optional

from sqlalchemy import Column, BigInteger, Index
from sqlalchemy import Integer, String
from vortex.Tuple import Tuple, addTupleType

from peek_abstract_chunked_index.private.tuples.ACIProcessorQueueTupleABC import \
    ACIProcessorQueueTupleABC
from .DeclarativeBase import DeclarativeBase
from ..PluginNames import livedbTuplePrefix

logger = logging.getLogger(__name__)


@addTupleType
class LiveDbRawValueQueue(Tuple, DeclarativeBase,
                          ACIProcessorQueueTupleABC):
    __tablename__ = 'LiveDbRawValueQueue'
    __tupleType__ = livedbTuplePrefix + __tablename__

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    modelSetId = Column(Integer, nullable=False)
    key = Column(String, nullable=False)
    rawValue = Column(String)

    @classmethod
    def sqlCoreLoad(cls, row):
        return LiveDbRawValueQueueTuple(row.id, row.modelSetId, row.key, row.rawValue)

    def ckiUniqueKey(self):
        """ See LiveDbRawValueQueueTuple.ckiUniqueKey """
        raise NotImplementedError()

    __table_args__ = (
        Index("idx_LiveDbRawValueQueue_all", id, modelSetId, key, rawValue, unique=False),
    )


@addTupleType
class LiveDbRawValueQueueTuple(Tuple):
    """ LiveDB Raw Value Queue Tuple

    This Tuple is designed to be as fast as possible to serialise and access
    as it's used heavily.

    """
    __tablename__ = 'LiveDbRawValueQueueSlots'
    __tupleType__ = livedbTuplePrefix + __tablename__

    __slots__ = ("data",)
    __rawJonableFields__ = ("data",)

    def __init__(self, id: int = None, modelSetId: int = None, key: str = None,
                 rawValue: Optional[str] = None):
        Tuple.__init__(self, data=(id, modelSetId, key, rawValue))

    @property
    def id(self) -> int:
        return self.data[0]

    @property
    def modelSetId(self) -> int:
        return self.data[1]

    @property
    def key(self) -> str:
        return self.data[2]

    @property
    def rawValue(self) -> Optional[str]:
        return self.data[3]

    @property
    def ckiUniqueKey(self):
        return "%s:%s" % (self.modelSetId, self.key)

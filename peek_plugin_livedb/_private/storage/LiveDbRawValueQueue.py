import logging
from typing import Optional

from sqlalchemy import Column, BigInteger
from sqlalchemy import Integer, String
from vortex.Tuple import Tuple, addTupleType

from .DeclarativeBase import DeclarativeBase
from ..PluginNames import livedbTuplePrefix

logger = logging.getLogger(__name__)


@addTupleType
class LiveDbRawValueQueue(Tuple, DeclarativeBase):
    __tablename__ = 'LiveDbRawValueQueue'
    __tupleType__ = livedbTuplePrefix + __tablename__

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    modelSetId = Column(Integer, primary_key=True)
    key = Column(String, primary_key=True)
    rawValue = Column(String, primary_key=True)


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
    def modelSetIdKey(self):
        return "%s:%s" % (self.modelSetId, self.key)

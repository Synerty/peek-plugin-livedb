import logging

from sqlalchemy import Column
from sqlalchemy import Integer, String
from vortex.Tuple import Tuple, addTupleType

from .DeclarativeBase import DeclarativeBase
from ..PluginNames import livedbTuplePrefix

logger = logging.getLogger(__name__)


@addTupleType
class LiveDbRawValueQueue(Tuple, DeclarativeBase):
    __tablename__ = 'LiveDbRawValueQueue'
    __tupleType__ = livedbTuplePrefix + __tablename__

    id = Column(Integer, primary_key=True, autoincrement=True)

    modelSetKey = Column(String, primary_key=True)
    key = Column(String, primary_key=True)
    rawValue = Column(String, primary_key=True)

    @property
    def modelSetKeyKey(self):
        return "%s:%s" % (self.modelSetKey, self.key)

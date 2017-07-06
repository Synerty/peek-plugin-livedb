import logging

from peek_plugin_livedb._private.PluginNames import livedbTuplePrefix
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer, String
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.orm.mapper import reconstructor
from sqlalchemy.sql.schema import Index, Sequence
from vortex.Tuple import Tuple, addTupleType, JSON_EXCLUDE

from .DeclarativeBase import DeclarativeBase
from .ModelSet import ModelSet

logger = logging.getLogger(__name__)


@addTupleType
class LiveDbTuple(Tuple, DeclarativeBase):
    __tupleTypeShort__ = 'LDK'
    __tablename__ = 'LiveDbItem'
    __tupleType__ = livedbTuplePrefix + __tablename__

    NUMBER_VALUE = 0
    STRING_VALUE = 1
    COLOR = 2
    LINE_WIDTH = 3
    LINE_STYLE = 4
    GROUP_PTR = 5

    id_seq = Sequence('LiveDbItem_id_seq',
                      metadata=DeclarativeBase.metadata,
                      schema=DeclarativeBase.metadata.schema)
    id = Column(Integer, id_seq, server_default=id_seq.next_value(),
                primary_key=True, nullable=False, doc="id")

    modelSetId = Column(Integer, ForeignKey('ModelSet.id', ondelete='CASCADE'),
                        doc=JSON_EXCLUDE, nullable=False)
    modelSet = relationship(ModelSet)

    # comment="The unique reference of the value we want from the live db"
    liveDbKey = Column(String, nullable=False, doc="key")

    # comment="The last value from the source"
    value = Column(String, doc="v")

    # comment="The PEEK value, converted to PEEK IDs if required (Color for example)"
    convertedValue = Column(String, doc=JSON_EXCLUDE)

    # comment="The type of data this value represents"
    dataType = Column(Integer, doc="dt")

    # Store custom props for this link
    props = Column(JSONB, doc=JSON_EXCLUDE)

    importHash = Column(String, doc=JSON_EXCLUDE)

    __table_args__ = (
        Index("idx_LiveDbDKey_importHash", importHash, unique=False),
        Index("idx_LiveDbDKey_modelSetId", modelSetId, unique=False),
        Index("idx_LiveDbDKey_liveDbKey", liveDbKey, unique=False),
    )

    @reconstructor
    def __init__(self, **kwargs):
        Tuple.__init__(self, **kwargs)
        self.props = {}

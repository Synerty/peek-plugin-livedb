
from peek_plugin_livedb._private.PluginNames import livedbTuplePrefix
from sqlalchemy import Column
from sqlalchemy import Integer, String
from vortex.Tuple import addTupleType, Tuple, TupleField

from .DeclarativeBase import DeclarativeBase


@addTupleType
class ModelSet(Tuple, DeclarativeBase):
    __tablename__ = 'ModelSet'
    __tupleType__ = livedbTuplePrefix + __tablename__

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    comment = Column(String)

    data = TupleField()


def getOrCreateModelSet(session, modelSetName):
    qry = session.query(ModelSet).filter(ModelSet.name == modelSetName)
    if not qry.count():
        session.add(ModelSet(name=modelSetName))
        session.commit()

    return qry.one()

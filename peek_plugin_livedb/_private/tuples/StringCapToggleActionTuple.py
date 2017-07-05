from vortex.Tuple import addTupleType, TupleField
from vortex.TupleAction import TupleActionABC

from peek_plugin_livedb._private.PluginNames import livedbTuplePrefix


@addTupleType
class StringCapToggleActionTuple(TupleActionABC):
    __tupleType__ = livedbTuplePrefix + "StringCapToggleActionTuple"

    stringIntId = TupleField()

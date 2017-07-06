from peek_plugin_livedb._private.PluginNames import livedbTuplePrefix

from peek_plugin_livedb._private.storage.LiveDbTuple import LiveDbTuple
from vortex.Tuple import Tuple, addTupleType


@addTupleType
class LiveDbDisplayValueTuple(Tuple):
    """ Live DB Display Value Tuple

    This tuple stores a value of a key in the Live DB database.

    """
    __tupleType__ = livedbTuplePrefix + 'LiveDbValueTuple'
    __slots__ = ("key", "dataType", "rawValue", "displayValue")

    DATA_TYPE_NUMBER_VALUE = LiveDbTuple.NUMBER_VALUE
    DATA_TYPE_STRING_VALUE = LiveDbTuple.STRING_VALUE
    DATA_TYPE_COLOR = LiveDbTuple.COLOR
    DATA_TYPE_LINE_WIDTH = LiveDbTuple.LINE_WIDTH
    DATA_TYPE_LINE_STYLE = LiveDbTuple.LINE_STYLE
    DATA_TYPE_GROUP_PTR = LiveDbTuple.GROUP_PTR

    def __init__(self, key=None, dataType=None, rawValue=None, displayValue=None):
        # DON'T CALL SUPER INIT
        self.key = key
        self.dataType = dataType
        self.rawValue = rawValue
        self.displayValue = displayValue

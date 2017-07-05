from peek_plugin_livedb._private.PluginNames import livedbTuplePrefix
from vortex.Tuple import Tuple, addTupleType, TupleField


@addTupleType
class DoSomethingTuple(Tuple):
    """ Do Something Tuple

    This tuple is publicly exposed and will be the result of the doSomething api call.
    """
    __tupleType__ = livedbTuplePrefix + 'DoSomethingTuple'

    #:  The result of the doSomething
    result = TupleField(defaultValue=dict)

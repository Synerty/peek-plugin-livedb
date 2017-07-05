from peek_plugin_base.PeekVortexUtil import peekServerName
from peek_plugin_livedb._private.PluginNames import livedbFilt
from peek_plugin_livedb._private.PluginNames import livedbActionProcessorName
from vortex.handler.TupleActionProcessorProxy import TupleActionProcessorProxy


def makeTupleActionProcessorProxy():
    return TupleActionProcessorProxy(
                tupleActionProcessorName=livedbActionProcessorName,
                proxyToVortexName=peekServerName,
                additionalFilt=livedbFilt)

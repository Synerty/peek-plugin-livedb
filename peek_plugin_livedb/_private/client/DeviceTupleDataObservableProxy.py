from peek_plugin_base.PeekVortexUtil import peekServerName
from peek_plugin_livedb._private.PluginNames import livedbFilt
from peek_plugin_livedb._private.PluginNames import livedbObservableName
from vortex.handler.TupleDataObservableProxyHandler import TupleDataObservableProxyHandler


def makeDeviceTupleDataObservableProxy():
    return TupleDataObservableProxyHandler(observableName=livedbObservableName,
                                           proxyToVortexName=peekServerName,
                                           additionalFilt=livedbFilt)

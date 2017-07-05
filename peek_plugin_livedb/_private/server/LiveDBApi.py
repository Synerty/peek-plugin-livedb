from peek_plugin_livedb._private.server.controller.MainController import MainController
from peek_plugin_livedb.server.LiveDBApiABC import LiveDBApiABC
from peek_plugin_livedb.tuples.DoSomethingTuple import DoSomethingTuple


class LiveDBApi(LiveDBApiABC):
    def __init__(self, mainController: MainController):
        self._mainController = mainController

    def doSomethingGood(self, somethingsDescription: str) -> DoSomethingTuple:
        """ Do Something Good

        Add a new task to the users device.

        :param somethingsDescription: An arbitrary string

        """

        # Here we could pass on the request to the self._mainController if we wanted.
        # EG self._mainController.somethingCalled(somethingsDescription)

        return DoSomethingTuple(result="SUCCESS : " + somethingsDescription)


    def shutdown(self):
        pass

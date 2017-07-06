from abc import ABCMeta, abstractmethod
from rx.subjects import Subject
from twisted.internet.defer import Deferred


class LiveDBReadApiABC(metaclass=ABCMeta):
    @abstractmethod
    def monitorLiveDbIdsObservable(self, modelSetName: str) -> Subject:
        """ Monitor Live DB ID Observable

       This observable emits list of IDs that the live db acquisition plugins should
       prioritise.

       This list will represent the IDs of the object that are currently being viewed.

        :param modelSetName:  The name of the model set to import the disps into

        :return: An observable that emits a list of integers.

        """

    @abstractmethod
    def liveDbTupleAdditionsObservable(self, modelSetName: str) -> Deferred:
        """ Live DB Tuple Added Items Observable

        Return an observable that fires when livedb items are added

        :param modelSetName: The name of the model set for the live db

        :return: An observable that fires when keys are removed from the live db
        :rtype: C{LiveDbValueTuple}

        """

    @abstractmethod
    def liveDbTupleRemovalsObservable(self, modelSetName: str) -> Deferred:
        """ Live DB Tuple Removed Items Observable

        Return an observable that fires when livedb items are removed

        :param modelSetName:  The name of the model set for the live db

        :return: An observable that fires when keys are removed from the live db
        :rtype: C{LiveDbValueTuple}

        """

    @abstractmethod
    def liveDbTuplesDeferredGenerator(self, modelSetName: str) -> Deferred:
        """ Live DB Tuples

        Return a generator that returns deferreds that are fired with chunks of the
         entire live db.

        This is served up in chunks to prevent ballooning the memory usage.

        Here is an example of how to use this method

        ::

                @inlineCallbacks
                def loadFromDiagramApi(diagramLiveDbApi:DiagramLiveDbApiABC):
                    deferredGenerator = diagramLiveDbApi.liveDbTuplesDefferedGenerator("modelName")

                    while True:
                        liveDbValueTuples :List[LiveDbValueTuple] = yield deferredGenerator()

                        # The end of the list is marked my an empty result
                        if not liveDbValueTuples:
                            break

                        # TODO, do something with this chunk of liveDbValueTuples


        :param modelSetName:  The name of the model set for the live db

        :return: A deferred that fires with a list of tuples
        :rtype: C{LiveDbValueTuple}

        """

    @abstractmethod
    def liveDbValueUpdatesObservable(self, modelSetName: str) -> Subject:
        """ Live DB Value Update Observable

        Return an observable that fires with lists of C{LiveDbValueTuple} tuples
        containing updates to live db values.

        :param modelSetName:  The name of the model set for the live db

        :return: An observable that fires when values are updated in the livedb
        :rtype: C{LiveDbValueTuple}

        """

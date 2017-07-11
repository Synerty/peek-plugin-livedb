from abc import ABCMeta, abstractmethod
from rx.subjects import Subject
from twisted.internet.defer import Deferred


class LiveDBReadApiABC(metaclass=ABCMeta):
    @abstractmethod
    def priorityLiveDbKeysObservable(self, modelSetName: str) -> Subject:
        """ Priority Live DB ID Observable

        This observable emits list of keys that the live db acquisition plugins should
        prioritise.

        This list will represent keys relating to the objects that are
        currently being viewed.

        :param modelSetName:  The name of the model set to import the disps into

        :return: An observable that emits a List[str].

        """

    @abstractmethod
    def itemAdditionsObservable(self, modelSetName: str) -> Subject:
        """ Live DB Tuple Added Items Observable

        Return an observable that fires when livedb items are added

        :param modelSetName: The name of the model set for the live db

        :return: An observable that fires when keys are removed from the live db
        :rtype: An observable that emits List[LiveDbDisplayValueTuple]

        """

    @abstractmethod
    def itemDeletionsObservable(self, modelSetName: str) -> Subject:
        """ Live DB Tuple Removed Items Observable

        Return an observable that fires when livedb items are removed

        :param modelSetName:  The name of the model set for the live db

        :return: An observable that fires when keys are removed from the live db
        :rtype: An observable that emits List[str]

        """

    @abstractmethod
    def bulkLoadDeferredGenerator(self, modelSetName: str) -> Deferred:
        """ Live DB Tuples

        Return a generator that returns deferreds that are fired with chunks of the
         entire live db.

        This is served up in chunks to prevent ballooning the memory usage.

        Here is an example of how to use this method

        ::

                @inlineCallbacks
                def loadFromDiagramApi(diagramLiveDbApi:DiagramLiveDbApiABC):
                    deferredGenerator = diagramLiveDbApi.bulkLoadDeferredGenerator("modelName")

                    while True:
                        liveDbValueTuples :List[LiveDbValueTuple] = yield next(deferredGenerator)

                        # The end of the list is marked my an empty result
                        if not liveDbValueTuples:
                            break

                        # TODO, do something with this chunk of liveDbValueTuples


        :param modelSetName:  The name of the model set for the live db

        :return: A deferred that fires with a list of tuples
        :rtype: C{LiveDbValueTuple}

        """

    @abstractmethod
    def rawValueUpdatesObservable(self, modelSetName: str) -> Subject:
        """ Raw Value Update Observable

        Return an observable that fires with lists of C{LiveDbRawValueTuple} tuples
        containing updates to live db values.

        :param modelSetName:  The name of the model set for the live db

        :return: An observable that fires when values are updated in the livedb
        :rtype: Subject[List[LiveDbRawValueTuple]]

        """

    @abstractmethod
    def displayValueUpdatesObservable(self, modelSetName: str) -> Subject:
        """ Display Value Update Observable

        Return an observable that fires with lists of C{LiveDbDisplayValueTuple} tuples
        containing updates to live db values.

        :param modelSetName:  The name of the model set for the live db

        :return: An observable that fires when values are updated in the livedb
        :rtype: An observable that fires with List[LiveDbDisplayValueTuple]

        """

from typing import Optional, Any

from .LrcFileCollection import LrcFileCollection
from .LrcFile import LrcFile
from .LrcSet import LrcSet
from lnst.Common.Parameters import Parameters


class LrcSets:
    """
    LrcSets is a container of multiple LrcSet instances and provides
    methods to get aggregated data from them based on specified filters
    """
    _data_collection: LrcFileCollection
    _data_sets: list[LrcSet]
    _data_filters: dict[str, Any]

    def __init__(self, collection: LrcFileCollection):
        self._data_collection = collection
        self._data_sets = []
        self._data_filters = {}

        for machines in collection.machines:
            self._data_sets.append(
                LrcSet(collection.get_data_files(machines), machines=machines)
            )

    @property
    def data_sets(self) -> list[LrcSet]:
        for data_set in self._data_sets:
            data_set.data_filters = self._data_filters

        return self._data_sets

    @property
    def data_filters(self) -> dict[str, Any]:
        return self._data_filters

    @data_filters.setter
    def data_filters(self, filters: dict[str, Any]):
        self._data_filters = filters

    @property
    def recipes(self) -> set[str]:
        return {
            data_file.recipe_name
            for data_set in self.data_sets
            for data_file in data_set.data_files
        }

    @property
    def recipe_params(self) -> list[Parameters]:
        return [
            data_file.recipe_params
            for data_set in self.data_sets
            for data_file in data_set.data_files
        ]

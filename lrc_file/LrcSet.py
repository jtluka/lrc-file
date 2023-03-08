from typing import Optional, Any

from .LrcFile import LrcFile


class LrcSet:
    """
    LrcSet represents a set of LrcFile objects related to specific
    LNST machine set that match a filtering criteria specified by data_filters.

    The filters are currently 'ip_versions' and 'perf_tests' recipe parameters.
    """
    _data_files: list[LrcFile]
    _data_filters: dict[str, Any]
    _machines: set[str]

    _filtered_data: Optional[list[dict[str, dict[str, float]]]] = None
    _filtered_data_files: Optional[list[LrcFile]] = None

    def __init__(
        self,
        data_files: list[LrcFile],
        machines: set[str],
        data_filters: Optional[dict[str, Any]] = None,
    ):
        self._data_files = data_files
        self._data_filters = data_filters or {}
        self._machines = machines

    @property
    def data_filters(self) -> dict[str, Any]:
        return self._data_filters

    @data_filters.setter
    def data_filters(self, filters: dict[str, Any]):
        self._clear_cache()
        self._data_filters = filters

    @property
    def machines(self) -> set[str]:
        return self._machines

    def _clear_cache(self):
        self._filtered_data = None
        self._filtered_data_files = None

    @property
    def data(self) -> list[dict[str, dict[str, float]]]:
        if self._filtered_data is not None:
            return self._filtered_data

        self._filtered_data = []
        for f in self.data_files:
            self._filtered_data.append(
                {"cpu": f.cpu_result_data, "flow": f.flow_result_data}
            )

        return self._filtered_data

    @property
    def data_files(self) -> list[LrcFile]:
        if self._filtered_data_files is not None:
            return self._filtered_data_files

        self._filtered_data_files = []
        if not self.data_filters:
            self._filtered_data_files.extend(self._data_files)
            return self._filtered_data_files

        for data_file in self._data_files:
            if (
                "recipe_name" in self.data_filters
                and self.data_filters["recipe_name"] != data_file.recipe_name
            ):
                continue

            file_matches_criteria = True
            for filter_key, filter_value in self.data_filters["params"].items():
                if filter_key in data_file.recipe_params:
                    recipe_value = getattr(data_file.recipe_params, filter_key)
                    value_type = type(recipe_value)
                    filter_value = value_type(filter_value)

                    if filter_value != recipe_value:
                        file_matches_criteria = False
                        break
                else:
                    file_matches_criteria = False
                    break

            if file_matches_criteria:
                self._filtered_data_files.append(data_file)

        return self._filtered_data_files

    def _metrics(self, metrics_type: str):
        ret_metrics: dict[str, list[float]] = {}
        for data_file in self.data_files:
            for metric_name, metric in getattr(data_file, metrics_type).items():
                ret_metric = ret_metrics.get(metric_name, [])
                ret_metric.append(metric)
                ret_metrics[metric_name] = ret_metric
        return ret_metrics

    @property
    def metrics(self) -> dict[str, list[float]]:
        return self._metrics("metrics")

    @property
    def evaluation_metrics(self) -> dict[str, list[float]]:
        return self._metrics("evaluation_metrics")

    @property
    def cpu_metrics(self) -> dict[str, list[float]]:
        return self._metrics("cpu_result_data")

    @property
    def cpu_evaluation_metrics(self) -> dict[str, list[float]]:
        return self._metrics("cpu_evaluation_data")

    @property
    def flow_metrics(self) -> dict[str, list[float]]:
        return self._metrics("flow_result_data")

    @property
    def flow_evaluation_metrics(self) -> dict[str, list[float]]:
        return self._metrics("flow_evaluation_data")

    @property
    def tc_metrics(self) -> dict[str, list[float]]:
        return self._metrics("tc_results_data")

    @property
    def tc_evaluation_metrics(self) -> dict[str, list[float]]:
        return self._metrics("tc_evaluation_data")


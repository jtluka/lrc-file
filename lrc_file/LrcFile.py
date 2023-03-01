from dataclasses import dataclass, field
from typing import Optional
import functools
import itertools

from lnst.Common import Parameters
from lnst.Controller.Recipe import RecipeRun, import_recipe_run
from lnst.Controller.RecipeResults import BaseResult
from lnst.RecipeCommon.Perf.Results import PerfResult
from lnst.RecipeCommon.Perf.Measurements.Results.FlowMeasurementResults import FlowMeasurementResults
from lnst.RecipeCommon.Perf.Measurements.Results.CPUMeasurementResults import CPUMeasurementResults
from lnst.RecipeCommon.Perf.Evaluators.BaselineEvaluator import BaselineEvaluationResult


@dataclass(frozen=True)
class Series:
    label: str
    data: list[float] = field(default_factory=list)


@dataclass(frozen=True)
class Run:
    label: str
    generator_series: list[Series] = field(default_factory=list, init=False)
    receiver_series: list[Series] = field(default_factory=list, init=False)


@dataclass(frozen=True)
class _Flow:
    is_aggregated: bool
    generator_data: list[list[float]] = field(default_factory=list)
    receiver_data: list[list[float]] = field(default_factory=list)


def _is_flow_measurement_result(result: BaseResult) -> bool:
    try:
        return "generator_flow_data" in result.data
    except TypeError:
        return False


def _is_cpu_measurement_result(result: BaseResult) -> bool:
    try:
        return "cpu" in result.data
    except TypeError:
        return False


def _get_flow_metrics(lnst_run: RecipeRun, evaluated_flow_metrics: list[str]) -> dict[str, float]:
    return {
        f"{i}_{key}": value.average
        for i, result in enumerate(lnst_run.results)
        if _is_flow_measurement_result(result)
        for key, value in result.data.items()
        if key in evaluated_flow_metrics
    }


def _get_cpu_metrics(lnst_run: RecipeRun, evaluated_cpu_metrics: list[str]) -> dict[str, float]:
    return {
        f"{i}_utilization": value.average
        for i, result in enumerate(lnst_run.results)
        if _is_cpu_measurement_result(result)
        for key, value in result.data.items()
        if key in evaluated_cpu_metrics
    }


def _get_cpu_data(lnst_run: RecipeRun) -> list[Run]:
    try:
        m1_results, m2_results = filter(_is_cpu_measurement_result, lnst_run.results)
    except ValueError:
        raise Exception("Number of CPU measurement results has to equal 2")

    number_of_runs = len(m1_results.data["cpu"])
    runs: list[Run] = []
    for run_index in range(number_of_runs):
        run = Run(label=f"iteration{run_index}")

        for results, run_series in [
            (m1_results.data, run.generator_series),
            (m2_results.data, run.receiver_series),
        ]:
            # individual cpus
            for cpu_name, cpu_data in results.items():
                number_of_intervals = len(cpu_data[run_index][0])

                # sum across all measurements for each second
                aggregated_cpu_data: list[float] = [
                    sum(measurement[interval].average for measurement in cpu_data[run_index])
                    for interval in range(number_of_intervals)
                ]

                new_series = Series(label=cpu_name, data=aggregated_cpu_data)
                run_series.append(new_series)
        runs.append(run)
    return runs


def _get_flow_data(lnst_run: RecipeRun) -> list[_Flow]:
    """Partially process flow data"""

    def aggregate_flows(aggregated: list[float], perf_results: list[PerfResult]) -> list[float]:
        """aggregate each element from `perf_results` into `aggregated`"""

        if not aggregated:
            return list(map(lambda x: x.average, perf_results))
        return list(map(lambda x, y: x + y.average, aggregated, perf_results))

    flows: list[_Flow] = []
    flow_results = filter(_is_flow_measurement_result, lnst_run.results)
    for flow_no, flow_result in enumerate(flow_results):
        try:
            is_aggregated: bool = flow_result.data["flow_results"].flow.aggregated_flow
        except (TypeError, KeyError):
            raise Exception("Could not find information whether flow is aggregated")

        generator_data: list[list[float]]
        receiver_data: list[list[float]]

        # aggregate values for each run together
        # aggregated flows have the data a level deeper
        if is_aggregated:
            generator_data = [
                functools.reduce(aggregate_flows, itertools.chain.from_iterable(run_data), [])
                for run_data in flow_result.data["generator_flow_data"]
            ]
            receiver_data = [
                functools.reduce(aggregate_flows, itertools.chain.from_iterable(run_data), [])
                for run_data in flow_result.data["receiver_flow_data"]
            ]
        else:
            generator_data = [
                functools.reduce(aggregate_flows, run_data, [])
                for run_data in flow_result.data["generator_flow_data"]
            ]
            receiver_data = [
                functools.reduce(aggregate_flows, run_data, [])
                for run_data in flow_result.data["receiver_flow_data"]
            ]
        flows.append(_Flow(is_aggregated, generator_data, receiver_data))
    return flows


class LrcFile:
    """
    LrcFile represents a file that has been exported from an LNST run
    using Recipe.export_recipe_run()
    """
    filename: str

    def __init__(
        self,
        filename: str,
        evaluated_flow_metrics: list[str] = [
            "generator_flow_data",
            "receiver_flow_data",
            "generator_cpu_data",
            "receiver_cpu_data",
        ],
        evaluated_cpu_metrics: list[str] = ["cpu"],
        delete_loaded_data: bool = True,
    ):
        self.filename = filename
        recipe_run: RecipeRun = import_recipe_run(self.filename)

        # instead of keeping the whole exported recipe run data, just save
        # the relevant parts of it
        self._flow_metrics = _get_flow_metrics(recipe_run, evaluated_flow_metrics)
        self._cpu_metrics = _get_cpu_metrics(recipe_run, evaluated_cpu_metrics)

        self._data = recipe_run
        self._cpu_data = _get_cpu_data(recipe_run)
        self._flow_data = _get_flow_data(recipe_run)

        self._recipe_params = recipe_run.recipe.params
        self._recipe_name = recipe_run.recipe.__class__.__name__
        self._match = recipe_run.match
        self._environ = recipe_run.environ

        if delete_loaded_data:
            # to reduce the memory foot print of the LrcFile, initialize
            # the following cached properties from the data, and discard
            # the data afterwards
            _, _ = self.cpu_evaluation_data, self.flow_evaluation_data
            self._data = None

    @property
    def data(self) -> Optional[RecipeRun]:
        return self._data

    @property
    def cpu_result_data(self) -> dict[str, float]:
        """
        Returns a dictionary containing average of CPU utilization measurement
        for host1/host2
        """
        return self._cpu_metrics

    @property
    def evaluation_results(self):
        return [
            result
            for result in self._data.results
            if isinstance(result, BaselineEvaluationResult)
        ]

    def _evaluation_data(self, result_type: type):
        """
        Returns dict of `result_type` metrics evaluated by BaselineEvaluator
        """
        evaluation_data = {}

        for result in self.evaluation_results:
            for comparison in result.data["comparisons"]:
                if not isinstance(comparison["current_result"], result_type):
                    continue

                evaluated_metric = comparison["metric_name"]
                evaluated_metric_name = evaluated_metric[4:]

                evaluation_data[evaluated_metric] = getattr(comparison["current_result"], evaluated_metric_name).average

        return evaluation_data

    @functools.cached_property
    def cpu_evaluation_data(self) -> dict[str, float]:
        """
            Returns CPU metrics with its values used during evaluation.
        """
        return self._evaluation_data(CPUMeasurementResults)

    @property
    def flow_result_data(self) -> dict[str, float]:
        """
        Returns a dictionary containing average of following measurements:
            generator_cpu_data
            generator_flow_data
            receiver_cpu_data
            receiver_flow_data
        """
        return self._flow_metrics

    @functools.cached_property
    def flow_evaluation_data(self) -> dict[str, float]:
        """
            Returns flow metrics with its values used during evaluation.
        """
        return self._evaluation_data(FlowMeasurementResults)

    @property
    def recipe_params(self) -> Parameters:
        return self._recipe_params

    @property
    def recipe_name(self) -> str:
        return self._recipe_name

    @property
    def ip_versions(self) -> tuple[str]:
        return self.recipe_params.ip_versions

    @property
    def perf_tests(self) -> tuple[str]:
        return self.recipe_params.perf_tests

    @property
    def machines(self) -> set[str]:
        return {
            m["hostname"]
            for m in self._match["machines"].values()
            if m["hostname"].startswith("wsfd")
        }

    @property
    def test_uuid(self) -> str:
        return self._environ["LNST_TEST_UUID"]

    @property
    def metrics(self) -> dict[str, float]:
        return {**self._flow_metrics, **self._cpu_metrics}

    @property
    def evaluation_metrics(self) -> dict[str, float]:
        return {**self.cpu_evaluation_data, **self.flow_evaluation_data}

    def get_raw_cpu_data(self) -> list[Run]:
        return self._cpu_data

    def get_raw_flow_data(
        self,
        aggregated_flows_only: bool = False,
        flow_whitelist: Optional[list[int]] = None,
    ) -> list[Run]:
        runs: list[Run] = []
        for run_no in range(len(self._flow_data[0].generator_data)):
            run = Run(label=f"iteration{run_no}")
            for flow_no, flow in enumerate(self._flow_data):

                # skip flows not in whitelist
                if flow_whitelist is not None and flow_no not in flow_whitelist:
                    continue

                # skip non-aggregated flows if only aggregated expected
                if aggregated_flows_only and not flow.is_aggregated:
                    continue

                generator_series = Series(
                    label=f"flow{flow_no}{'(agg)' if flow.is_aggregated else ''}",
                    data=flow.generator_data[run_no],
                    )
                receiver_series = Series(
                    label=f"flow{flow_no}{'(agg)' if flow.is_aggregated else ''}",
                    data=flow.receiver_data[run_no],
                )

                run.generator_series.append(generator_series)
                run.receiver_series.append(receiver_series)
            runs.append(run)
        return runs

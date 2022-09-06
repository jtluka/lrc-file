import re


def compare_lnst_runs(run1, run2, run_info=False, ignored_params=[]):
    errors = validate_runs_comparable(run1, run2, ignored_params)

    if errors:
        raise Exception(
            "Runs not comparable, errors:\n{}".format("\n".join(errors))
        )
    else:
        print("Comparing runs:")
        if run_info:
            print("\n".join(["\t" + line for line in format_run_info(run1)]))
            print("\n".join(["\t" + line for line in format_run_info(run2)]))

        print("Simple run results comparison:")
        simple_comparison = simple_compare_run_results(run1, run2)
        if len(simple_comparison):
            print("\n".join(["\t" + line for line in simple_comparison]))
        else:
            print("\tRun results simply match")

        print("Measurement comparison:")
        measurement_comparison = compare_run_measurements(run1, run2)
        print("\n".join(["\t" + line for line in measurement_comparison]))


def validate_runs_comparable(run1, run2, ignored_params):
    errors = []
    if run1.recipe_name != run2.recipe_name:
        errors.append(
            "Run1 recipe {} != Run2 recipe {}".format(
                run1.recipe_name, run2.recipe_name
            )
        )

    param_errors = compare_recipe_params(
        run1.recipe_params, run2.recipe_params, ignored_params
    )
    if param_errors:
        errors.append("Run1 recipe_params != Run2 recipe_params, errors:")
        errors.extend(["\t" + error for error in param_errors])

    return errors


def format_run_info(run):
    result = [
        "Run info:",
        "Beaker url: " + run.beaker_url,
        "Machine pair: "
        + str([m for m in run.machines if m.startswith("wsfd")]),
        "Test uuid: " + run.test_uuid,
        "Recipe name: " + run.recipe_name,
        "Recipe params:",
    ]
    for key in sorted(run.recipe_params._to_dict().keys()):
        result.append("\t{}={}".format(key, getattr(run.recipe_params, key)))
    return result


def compare_recipe_params(params1, params2, ignored_params):
    d1 = params1._to_dict()
    d2 = params2._to_dict()

    errors = []
    for key, val in d1.items():
        if key in ignored_params:
            continue
        if key not in d2:
            errors.append(
                "Param {} not present in run2, but present in run1.".format(key)
            )
            continue
        elif d1[key] != d2[key]:
            errors.append(
                "Param {} values different in runs: {} != {}".format(
                    key, d1[key], d2[key]
                )
            )
            del d2[key]
        else:
            del d2[key]

    for key, val in d2.items():
        if key in ignored_params:
            continue
        errors.append("Param {} not present in run1, but present in run2.".format(key))

    return errors


def simple_compare_run_results(run1, run2):
    errors = []
    if len(run1.run_results) != len(run2.run_results):
        return [
            "Runs have different number of results, run1={}, run2={}".format(
                len(run1.run_results), len(run2.run_results)
            )
        ]
    for result1, result2 in zip(run1.run_results, run2.run_results):
        errors.extend(simple_compare_results(result1, result2))

    return errors


def simple_compare_results(result1, result2):
    errors = []
    if type(result1) != type(result2):
        errors.append(
            "Run1 and Run2 result types don't match: {} != {}".format(
                type(result1), type(result2)
            )
        )
        return errors

    if result1.success != result2.success:
        errors.append(
            "Run1 {} != Run2 {} for results {} {}".format(
                result1.success, result2.success, result1, result2
            )
        )
    return errors


def compare_run_measurements(run1, run2):
    results = []
    results.extend(compare_flow_run_results(run1, run2))
    results.extend(compare_cpu_run_results(run1, run2))
    return results


def compare_flow_run_results(run1, run2):
    results = []
    results1 = run1.flow_performance_results
    results2 = run2.flow_performance_results
    for result1, result2 in generate_flow_measurement_pairs(results1, results2):
        results.append("Flow comparison:")
        results.extend(
            ["\t" + line for line in compare_flow_results(result1, result2)]
        )
    return results


def generate_flow_measurement_pairs(flows1, flows2):
    # TODO actually check the comparability of the flows
    return zip(flows1, flows2)


def compare_flow_results(result1, result2):
    data1 = result1.data
    data2 = result2.data

    results = [
        "generator_throughput run1 run2 difference={:.2%}, abs={:.2f}; {:.2f}, deviations={:.2%}; {:.2%}".format(
            calculate_ratio(
                data1["generator_flow_data"].average,
                data2["generator_flow_data"].average,
            )
            - 1,
            data1["generator_flow_data"].average,
            data2["generator_flow_data"].average,
            calculate_ratio(
                data1["generator_flow_data"].std_deviation,
                data1["generator_flow_data"].average,
            ),
            calculate_ratio(
                data2["generator_flow_data"].std_deviation,
                data2["generator_flow_data"].average,
            ),
        ),
        "receiver_throughput run1 run2 difference={:.2%}, abs={:.2f}; {:.2f}, deviations={:.2%}; {:.2%}".format(
            calculate_ratio(
                data1["receiver_flow_data"].average,
                data2["receiver_flow_data"].average,
            )
            - 1,
            data1["receiver_flow_data"].average,
            data2["receiver_flow_data"].average,
            calculate_ratio(
                data1["receiver_flow_data"].std_deviation,
                data1["receiver_flow_data"].average,
            ),
            calculate_ratio(
                data2["receiver_flow_data"].std_deviation,
                data2["receiver_flow_data"].average,
            ),
        ),
        "generator_cpu_data run1 run2 difference={:.2%}, abs={:.2f}; {:.2f}, deviations={:.2%}; {:.2%}".format(
            calculate_ratio(
                data1["generator_cpu_data"].average,
                data2["generator_cpu_data"].average,
            )
            - 1,
            data1["generator_cpu_data"].average,
            data2["generator_cpu_data"].average,
            calculate_ratio(
                data1["generator_cpu_data"].std_deviation,
                data1["generator_cpu_data"].average,
            ),
            calculate_ratio(
                data2["generator_cpu_data"].std_deviation,
                data2["generator_cpu_data"].average,
            ),
        ),
        "receiver_cpu_data run1 run2 difference={:.2%}, abs={:.2f}; {:.2f}, deviations={:.2%}; {:.2%}".format(
            calculate_ratio(
                data1["receiver_cpu_data"].average,
                data2["receiver_cpu_data"].average,
            )
            - 1,
            data1["receiver_cpu_data"].average,
            data2["receiver_cpu_data"].average,
            calculate_ratio(
                data1["receiver_cpu_data"].std_deviation,
                data1["receiver_cpu_data"].average,
            ),
            calculate_ratio(
                data2["receiver_cpu_data"].std_deviation,
                data2["receiver_cpu_data"].average,
            ),
        ),
    ]
    return results


def compare_cpu_run_results(run1, run2):
    results = []
    results1 = run1.cpu_performance_results
    results2 = run2.cpu_performance_results
    for result1, result2 in generate_cpu_measurement_pairs(results1, results2):
        host1 = get_cpu_hostid(result1)
        host2 = get_cpu_hostid(result2)
        results.append("Hosts: {} vs {}".format(host1, host2))
        results.extend(
            ["\t" + line for line in compare_cpu_results(result1, result2)]
        )
    return results


def generate_cpu_measurement_pairs(cpus1, cpus2):
    sorted_cpus1 = sorted(cpus1, key=get_cpu_hostid)
    sorted_cpus2 = sorted(cpus2, key=get_cpu_hostid)
    # TODO actually check comparability of cpu measurements
    return zip(sorted_cpus1, sorted_cpus2)


def compare_cpu_results(result1, result2):
    results = []
    data1 = result1.data
    data2 = result2.data

    for core1, core2 in zip(data1.items(), data2.items()):
        results.append(
            "core1 {} core2 {} difference={:.2%}, abs={:.2f}; {:.2f}, deviations={:.2%}; {:.2%}".format(
                core1[0],
                core2[0],
                calculate_ratio(core1[1].average, core2[1].average) - 1,
                core1[1].average,
                core2[1].average,
                calculate_ratio(core1[1].std_deviation, core1[1].average),
                calculate_ratio(core2[1].std_deviation, core2[1].average),
            ),
        )
    return results


def get_cpu_hostid(result):
    regex = re.compile("CPU Utilization on host (.*):")
    for line in result.description.split("\n"):
        m = regex.match(line)
        if m is not None:
            return m.group(1)

    raise Exception("could not find hostid in CPU measurement result!")


def calculate_ratio(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return float("inf") if a >= 0 else float("-inf")

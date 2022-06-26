import pytest

from dagster import Failure, graph
from docs_snippets.concepts.ops_jobs_graphs.op_events import (
    my_asset_op,
    my_expectation_op,
    my_failure_metadata_op,
    my_failure_op,
    my_metadata_expectation_op,
    my_metadata_output,
    my_multiple_generic_output_op,
    my_op_yields,
    my_output_generic_op,
    my_output_op,
    my_retry_op,
)


def execute_op_in_graph(an_op, **kwargs):
    @graph
    def my_graph():
        return an_op(**kwargs) if kwargs else an_op()

    result = my_graph.execute_in_process()
    return result


def generate_stub_input_values(op):
    default_values = {"String": "abc", "Int": 1, "Any": []}

    input_defs = op.input_defs
    return {
        input_def.name: default_values[
            str(input_def.dagster_type.display_name)
        ]
        for input_def in input_defs
    }


def test_ops_compile_and_execute():
    ops = [
        my_metadata_output,
        my_metadata_expectation_op,
        my_retry_op,
        my_asset_op,
        my_output_generic_op,
        my_expectation_op,
        my_multiple_generic_output_op,
        my_output_op,
        my_op_yields,
    ]

    for op in ops:
        input_values = generate_stub_input_values(op)
        result = execute_op_in_graph(op, **input_values)
        assert result
        assert result.success


def test_failure_op():
    with pytest.raises(Failure):
        execute_op_in_graph(my_failure_op)


def test_failure_metadata_op():
    with pytest.raises(Failure):
        execute_op_in_graph(my_failure_metadata_op)

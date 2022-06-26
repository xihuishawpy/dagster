import random
from collections import defaultdict

from dagster import (
    DependencyDefinition,
    Field,
    InputDefinition,
    Output,
    OutputDefinition,
    PipelineDefinition,
    SolidDefinition,
)
from dagster import _check as check


def generate_solid(solid_id, num_inputs, num_outputs, num_cfg):
    def compute_fn(_context, **_kwargs):
        for i in range(num_outputs):
            yield Output(i, f"out_{i}")

    config = {}
    for i in range(num_cfg):
        config[f"field_{i}"] = Field(str, is_required=False)

    return SolidDefinition(
        name=solid_id,
        input_defs=[
            InputDefinition(name="in_{}".format(i), default_value="default")
            for i in range(num_inputs)
        ],
        output_defs=[OutputDefinition(name="out_{}".format(i)) for i in range(num_outputs)],
        compute_fn=compute_fn,
        config_schema=config,
    )


def generate_pipeline(name, size, connect_factor=1.0):
    check.int_param(size, "size")
    check.invariant(size > 3, "Can not create pipelines with less than 3 nodes")
    check.float_param(connect_factor, "connect_factor")

    random.seed(name)

    # generate nodes
    solids = {}
    for i in range(size):
        num_inputs = random.randint(1, 3)
        num_outputs = random.randint(1, 3)
        num_cfg = random.randint(0, 5)
        solid_id = f"{name}_solid_{i}"
        solids[solid_id] = generate_solid(
            solid_id=solid_id,
            num_inputs=num_inputs,
            num_outputs=num_outputs,
            num_cfg=num_cfg,
        )

    solid_ids = list(solids.keys())
    # connections
    deps = defaultdict(dict)
    for _ in range(int(size * connect_factor)):
        # choose output
        out_idx = random.randint(0, len(solid_ids) - 2)
        out_solid_id = solid_ids[out_idx]
        output_solid = solids[out_solid_id]
        output_name = output_solid.output_defs[
            random.randint(0, len(output_solid.output_defs) - 1)
        ].name

        # choose input
        in_idx = random.randint(out_idx + 1, len(solid_ids) - 1)
        in_solid_id = solid_ids[in_idx]
        input_solid = solids[in_solid_id]
        input_name = input_solid.input_defs[random.randint(0, len(input_solid.input_defs) - 1)].name

        # map
        deps[in_solid_id][input_name] = DependencyDefinition(out_solid_id, output_name)

    return PipelineDefinition(name=name, solid_defs=list(solids.values()), dependencies=deps)

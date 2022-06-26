from typing import List, NamedTuple, Optional, Union

import dagster._check as check

from .graph_definition import GraphDefinition
from .mode import DEFAULT_MODE_NAME
from .pipeline_definition import PipelineDefinition
from .unresolved_asset_job_definition import UnresolvedAssetJobDefinition


class RepoRelativeTarget(NamedTuple):
    """
    The thing to be executed by a schedule or sensor, selecting by name a pipeline in the same repository.
    """

    pipeline_name: str
    mode: str
    solid_selection: Optional[List[str]]


class DirectTarget(
    NamedTuple(
        "_DirectTarget",
        [("target", Union[GraphDefinition, PipelineDefinition, UnresolvedAssetJobDefinition])],
    )
):
    """
    The thing to be executed by a schedule or sensor, referenced directly and loaded
    in to any repository the container is included in.
    """

    def __new__(
        cls, target: Union[GraphDefinition, PipelineDefinition, UnresolvedAssetJobDefinition]
    ):
        check.inst_param(
            target, "target", (GraphDefinition, PipelineDefinition, UnresolvedAssetJobDefinition)
        )

        if (
            isinstance(target, PipelineDefinition)
            and len(target.mode_definitions) != 1
        ):
            check.failed(
                "Only graphs, jobs, and single-mode pipelines are valid "
                "execution targets from a schedule or sensor. Please see the "
                f"following guide to migrate your pipeline '{target.name}': "
                "https://docs.dagster.io/guides/dagster/graph_job_op#migrating-to-ops-jobs-and-graphs"
            )

        return super().__new__(
            cls,
            target,
        )

    @property
    def pipeline_name(self) -> str:
        return self.target.name

    @property
    def mode(self) -> str:
        return (
            self.target.mode_definitions[0].name
            if isinstance(self.target, PipelineDefinition)
            else DEFAULT_MODE_NAME
        )

    @property
    def solid_selection(self):
        # open question on how to direct target subset pipeline
        return None

    def load(self) -> Union[PipelineDefinition, GraphDefinition, UnresolvedAssetJobDefinition]:
        return self.target

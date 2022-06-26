import logging
import os
import subprocess
from typing import List, Tuple

from dagster_buildkite.defines import DO_COVERAGE
from dagster_buildkite.steps.coverage import build_coverage_step
from dagster_buildkite.steps.dagit_ui import build_dagit_ui_steps
from dagster_buildkite.steps.dagster import build_dagster_steps
from dagster_buildkite.steps.docs import build_docs_steps
from dagster_buildkite.steps.integration import build_integration_steps
from dagster_buildkite.steps.trigger import build_trigger_step
from dagster_buildkite.steps.wait import build_wait_step
from dagster_buildkite.utils import BuildkiteStep, is_feature_branch, safe_getenv

_DAGIT_PATHES = ("js_modules/dagit",)
_DOCS_PATHES = ("examples", "docs")

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=os.getenv("LOGLEVEL", "INFO"),
    datefmt="%Y-%m-%d %H:%M:%S",
)


def build_dagster_oss_main_steps() -> List[BuildkiteStep]:

    branch_name = safe_getenv("BUILDKITE_BRANCH")
    commit_hash = safe_getenv("BUILDKITE_COMMIT")
    build_creator_email = os.getenv("BUILDKITE_BUILD_CREATOR_EMAIL")
    oss_contribution = os.getenv("OSS_CONTRIBUTION")
    do_coverage = DO_COVERAGE
    dagit_ui_only_diff = _is_path_only_diff(paths=_DAGIT_PATHES)
    docs_only_diff = _is_path_only_diff(paths=_DOCS_PATHES)

    steps: List[BuildkiteStep] = []

    # Trigger a build on the internal pipeline for Elementl dev PRs. Feature branches use the
    # `oss-internal-compatibility` pipeline and always use the internal master branch.
    # Master/release branches use the full `nternal` pipeline and run on the matching internal
    # branch.
    if (
        build_creator_email
        and build_creator_email.endswith("@elementl.com")
        and build_creator_email != "devtools@elementl.com"
        and not oss_contribution
    ):
        if is_feature_branch(branch_name):
            pipeline_name = "oss-internal-compatibility"
            trigger_branch = "master"
            async_step = False
        else:
            pipeline_name = "internal"
            trigger_branch = branch_name  # build on matching internal release branch
            async_step = True

        steps.append(
            build_trigger_step(
                pipeline=pipeline_name,
                trigger_branch=trigger_branch,
                async_step=async_step,
                env={
                    "DAGSTER_BRANCH": branch_name,
                    "DAGSTER_COMMIT_HASH": commit_hash,
                    "DAGIT_ONLY_OSS_CHANGE": "1" if dagit_ui_only_diff else "",
                },
            ),
        )

    # Skip non-dagit-ui steps if we are on a feature branch with only dagit-ui (web app) changes.
    logging.info(f"dagit_ui_only: {dagit_ui_only_diff}, docs_only: {docs_only_diff}")
    if is_feature_branch(branch_name) and dagit_ui_only_diff:
        steps += build_dagit_ui_steps()

    # Skip non-docs steps if we are on a feature branch with only docs changes.
    elif is_feature_branch(branch_name) and docs_only_diff:
        steps += build_docs_steps()

    # Full pipeline.
    else:
        steps += build_docs_steps()
        steps += build_dagit_ui_steps()
        steps += build_dagster_steps()
        steps += build_integration_steps()

        if do_coverage:
            steps.append(build_wait_step())
            steps.append(build_coverage_step())

    return steps


def _is_path_only_diff(paths: Tuple[str, ...]) -> bool:
    base_branch = safe_getenv("BUILDKITE_PULL_REQUEST_BASE_BRANCH")

    try:
        pr_commit = safe_getenv("BUILDKITE_COMMIT")
        origin_base = f"origin/{base_branch}"
        diff_files = (
            subprocess.check_output(["git", "diff", origin_base, pr_commit, "--name-only"])
            .decode("utf-8")
            .strip()
            .split("\n")
        )
        return all(filepath.startswith(paths) for (filepath) in diff_files)

    except subprocess.CalledProcessError:
        return False

# pylint: disable=print-call

import os
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

import docker
import packaging
import pytest
import requests
from dagster_graphql import DagsterGraphQLClient

from dagster import file_relative_path
from dagster.core.storage.pipeline_run import PipelineRunStatus

DAGSTER_CURRENT_BRANCH = "current_branch"
MAX_TIMEOUT_SECONDS = 20
IS_BUILDKITE = os.getenv("BUILDKITE") is not None
EARLIEST_TESTED_RELEASE = os.getenv("EARLIEST_TESTED_RELEASE")
MOST_RECENT_RELEASE_PLACEHOLDER = "most_recent"

pytest_plugins = ["dagster_test.fixtures"]


# pylint: disable=redefined-outer-name
RELEASE_TEST_MAP = {
    "dagit-earliest-release": [EARLIEST_TESTED_RELEASE, DAGSTER_CURRENT_BRANCH],
    "user-code-earliest-release": [DAGSTER_CURRENT_BRANCH, EARLIEST_TESTED_RELEASE],
    "dagit-latest-release": [MOST_RECENT_RELEASE_PLACEHOLDER, DAGSTER_CURRENT_BRANCH],
    "user-code-latest-release": [DAGSTER_CURRENT_BRANCH, MOST_RECENT_RELEASE_PLACEHOLDER],
}


def assert_run_success(client, run_id):
    start_time = time.time()
    while True:
        if time.time() - start_time > MAX_TIMEOUT_SECONDS:
            raise Exception("Timed out waiting for launched run to complete")

        status = client.get_run_status(run_id)
        assert status and status != PipelineRunStatus.FAILURE
        if status == PipelineRunStatus.SUCCESS:
            break

        time.sleep(1)


@pytest.fixture(name="dagster_most_recent_release", scope="session")
def dagster_most_recent_release():
    res = requests.get("https://pypi.org/pypi/dagster/json")
    module_json = res.json()
    releases = module_json["releases"]
    release_versions = [packaging.version.parse(release) for release in releases.keys()]
    for release_version in reversed(sorted(release_versions)):
        if not release_version.is_prerelease:
            return str(release_version)


@pytest.fixture(
    params=[
        pytest.param(value, marks=getattr(pytest.mark, key), id=key)
        for key, value in RELEASE_TEST_MAP.items()
    ],
)
def release_test_map(request, dagster_most_recent_release):
    dagit_version = request.param[0]
    if dagit_version == MOST_RECENT_RELEASE_PLACEHOLDER:
        dagit_version = dagster_most_recent_release
    user_code_version = request.param[1]
    if user_code_version == MOST_RECENT_RELEASE_PLACEHOLDER:
        user_code_version = dagster_most_recent_release

    return {"dagit": dagit_version, "user_code": user_code_version}


@contextmanager
def docker_service_up(docker_compose_file, build_args=None):
    if IS_BUILDKITE:
        try:
            yield  # buildkite pipeline handles the service
        finally:

            # collect logs from the containers and upload to buildkite
            client = docker.client.from_env()
            containers = client.containers.list()

            current_test = os.environ.get("PYTEST_CURRENT_TEST").split(":")[-1].split(" ")[0]
            logs_dir = f".docker_logs/{current_test}"

            # delete any existing logs
            p = subprocess.Popen(["rm", "-rf", "{dir}".format(dir=logs_dir)])
            p.communicate()
            assert p.returncode == 0

            Path(logs_dir).mkdir(parents=True, exist_ok=True)

            for c in containers:
                with open(
                    "{dir}/{container}-logs.txt".format(dir=logs_dir, container=c.name),
                    "w",
                    encoding="utf8",
                ) as log:
                    p = subprocess.Popen(
                        ["docker", "logs", c.name],
                        stdout=log,
                        stderr=log,
                    )
                    p.communicate()
                    print(f"container({c.name}) logs dumped")
                    if p.returncode != 0:
                        q = subprocess.Popen(
                            ["docker", "logs", c.name],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                        )
                        stdout, stderr = q.communicate()
                        print(f"{c.name} container log dump failed with stdout: ", stdout)
                        print(f"{c.name} container logs dump failed with stderr: ", stderr)

            p = subprocess.Popen(
                [
                    "buildkite-agent",
                    "artifact",
                    "upload",
                    "{dir}/**/*".format(dir=logs_dir),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, stderr = p.communicate()
            print("Buildkite artifact added with stdout: ", stdout)
            print("Buildkite artifact added with stderr: ", stderr)
        return

    try:
        subprocess.check_output(["docker-compose", "-f", docker_compose_file, "stop"])
        subprocess.check_output(["docker-compose", "-f", docker_compose_file, "rm", "-f"])
    except subprocess.CalledProcessError:
        pass

    build_process = subprocess.Popen(
        [file_relative_path(docker_compose_file, "./build.sh")]
        + (build_args or [])
    )

    build_process.wait()
    assert build_process.returncode == 0

    up_process = subprocess.Popen(["docker-compose", "-f", docker_compose_file, "up", "--no-start"])
    up_process.wait()
    assert up_process.returncode == 0

    start_process = subprocess.Popen(["docker-compose", "-f", docker_compose_file, "start"])
    start_process.wait()
    assert start_process.returncode == 0

    try:
        yield
    finally:
        subprocess.check_output(["docker-compose", "-f", docker_compose_file, "stop"])
        subprocess.check_output(["docker-compose", "-f", docker_compose_file, "rm", "-f"])


@pytest.fixture
def graphql_client(release_test_map, retrying_requests):
    dagit_host = os.environ.get("BACKCOMPAT_TESTS_DAGIT_HOST", "localhost")

    dagit_version = release_test_map["dagit"]
    user_code_version = release_test_map["user_code"]

    with docker_service_up(
        os.path.join(os.getcwd(), "dagit_service", "docker-compose.yml"),
        build_args=[dagit_version, user_code_version],
    ):
        result = retrying_requests.get(f"http://{dagit_host}:3000/dagit_info")
        assert result.json().get("dagit_version")
        yield DagsterGraphQLClient(dagit_host, port_number=3000)


def test_backcompat_deployed_pipeline(graphql_client):
    assert_runs_and_exists(graphql_client, "the_pipeline")


def test_backcompat_deployed_pipeline_subset(graphql_client):
    assert_runs_and_exists(graphql_client, "the_pipeline", subset_selection=["my_solid"])


def test_backcompat_deployed_job(graphql_client):
    assert_runs_and_exists(graphql_client, "the_job")


def test_backcompat_deployed_job_subset(graphql_client):
    assert_runs_and_exists(graphql_client, "the_job", subset_selection=["my_op"])


def assert_runs_and_exists(client: DagsterGraphQLClient, name, subset_selection=None):
    run_id = client.submit_pipeline_execution(
        pipeline_name=name,
        mode="default",
        run_config={},
        solid_selection=subset_selection,
    )
    assert_run_success(client, run_id)

    locations = (
        client._get_repo_locations_and_names_with_pipeline(  # pylint: disable=protected-access
            pipeline_name=name
        )
    )
    assert len(locations) == 1
    assert locations[0].pipeline_name == name

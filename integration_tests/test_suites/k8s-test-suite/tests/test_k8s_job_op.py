import pytest
from dagster_k8s import k8s_job_op

from dagster import job


@pytest.mark.default
def test_k8s_job_op(namespace, cluster_provider):
    first_op = k8s_job_op.configured(
        {
            "image": "busybox",
            "command": ["/bin/sh", "-c"],
            "args": ["echo HI"],
            "namespace": namespace,
            "load_incluster_config": False,
            "kubeconfig_file": cluster_provider.kubeconfig_file,
        },
        name="first_op",
    )
    second_op = k8s_job_op.configured(
        {
            "image": "busybox",
            "command": ["/bin/sh", "-c"],
            "args": ["echo GOODBYE"],
            "namespace": namespace,
            "load_incluster_config": False,
            "kubeconfig_file": cluster_provider.kubeconfig_file,
        },
        name="second_op",
    )

    @job
    def my_full_job():
        second_op(first_op())

    my_full_job.execute_in_process()

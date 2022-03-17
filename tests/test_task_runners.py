import subprocess

import prefect
import pytest
import ray
import ray.cluster_utils


import tests  # Import the current tests directory to send to ray workers
from prefect_ray import RayTaskRunner


@pytest.fixture(scope="module")
@pytest.mark.service("ray")
def machine_ray_instance():
    """
    Starts a ray instance for the current machine
    """
    subprocess.check_call(
        ["ray", "start", "--head", "--include-dashboard", "False"],
        cwd=str(prefect.__root_path__),
    )
    try:
        yield "ray://127.0.0.1:10001"
    finally:
        subprocess.run(["ray", "stop"])


@pytest.fixture
@pytest.mark.service("ray")
def ray_task_runner_with_existing_cluster(
    machine_ray_instance,
    use_hosted_orion,
    hosted_orion_api,
):
    """
    Generate a ray task runner that's connected to a ray instance running in a separate
    process.

    This tests connection via `ray://` which is a client-based connection.
    """
    yield RayTaskRunner(
        address=machine_ray_instance,
        init_kwargs={
            "runtime_env": {
                # Ship the 'tests' module to the workers or they will not be able to
                # deserialize test tasks / flows
                "py_modules": [tests]
            }
        },
    )


@pytest.fixture(scope="module")
@pytest.mark.service("ray")
def inprocess_ray_cluster():
    """
    Starts a ray cluster in-process
    """
    cluster = ray.cluster_utils.Cluster(initialize_head=True)
    try:
        cluster.add_node()  # We need to add a second node for parallelism
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture
@pytest.mark.service("ray")
def ray_task_runner_with_inprocess_cluster(
    inprocess_ray_cluster,
    use_hosted_orion,
    hosted_orion_api,
):
    """
    Generate a ray task runner that's connected to an in-process cluster.

    This tests connection via 'localhost' which is not a client-based connection.
    """

    yield RayTaskRunner(
        address=inprocess_ray_cluster.address,
        init_kwargs={
            "runtime_env": {
                # Ship the 'tests' module to the workers or they will not be able to
                # deserialize test tasks / flows
                "py_modules": [tests]
            }
        },
    )

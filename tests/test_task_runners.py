import uuid
import subprocess
import asyncio

import warnings
import prefect
import pytest
import ray
import ray.cluster_utils

import tests
from prefect.testing.fixtures import use_hosted_orion, hosted_orion_api
from prefect_ray import RayTaskRunner
from prefect.orion.schemas.core import TaskRun
from prefect.task_runners import TaskConcurrencyType
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
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
def default_ray_task_runner():
    with warnings.catch_warnings():
        # Ray does not properly close resources and we do not want their warnings to
        # bubble into our test suite
        # https://github.com/ray-project/ray/pull/22419
        warnings.simplefilter("ignore", ResourceWarning)

        yield RayTaskRunner()


@pytest.fixture
def ray_task_runner_with_existing_cluster(
    machine_ray_instance, use_hosted_orion, hosted_orion_api
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
def ray_task_runner_with_inprocess_cluster(
    inprocess_ray_cluster, use_hosted_orion, hosted_orion_api
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


@pytest.fixture
def ray_task_runner_with_temporary_cluster(use_hosted_orion, hosted_orion_api):
    """
    Generate a ray task runner that creates a temporary cluster.

    This tests connection via 'localhost' which is not a client-based connection.
    """

    yield RayTaskRunner(
        init_kwargs={
            "runtime_env": {
                # Ship the 'tests' module to the workers or they will not be able to
                # deserialize test tasks / flows
                "py_modules": [tests]
            }
        },
    )


@pytest.mark.service("ray")
class TestRayTaskRunner(TaskRunnerStandardTestSuite):
    @pytest.fixture(
        params=[
            default_ray_task_runner,
            ray_task_runner_with_existing_cluster,
            ray_task_runner_with_inprocess_cluster,
        ]
    )
    def task_runner(self, request):
        yield request.getfixturevalue(
            request.param._pytestfixturefunction.name or request.param.__name__
        )

    # Ray wraps the exception, interrupts will result in "Cancelled" tasks
    # or "Killed" workers while normal errors will result in a "RayTaskError".
    # We care more about the crash detection and
    # lack of re-raise here than the equality of the exception.
    @pytest.mark.parametrize("exception", [KeyboardInterrupt(), ValueError("test")])
    async def test_wait_captures_exceptions_as_crashed_state(
        self, task_runner, exception
    ):
        """
        Ray wraps the exception, interrupts will result in "Cancelled" tasks
        or "Killed" workers while normal errors will result in a "RayTaskError".
        We care more about the crash detection and
        lack of re-raise here than the equality of the exception.
        """
        if task_runner.concurrency_type != TaskConcurrencyType.PARALLEL:
            pytest.skip(
                f"This will raise for {task_runner.concurrency_type} task runners."
            )

        task_run = TaskRun(flow_run_id=uuid4(), task_key="foo", dynamic_key="bar")

        async def fake_orchestrate_task_run():
            raise exception

        async with task_runner.start():
            future = await task_runner.submit(
                task_run=task_run, run_fn=fake_orchestrate_task_run, run_kwargs={}
            )

            state = await task_runner.wait(future, 5)
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert state.name == "Crashed"

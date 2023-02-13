import asyncio
import logging
import subprocess
import sys
import time
import warnings
from functools import partial
from uuid import uuid4

import prefect
import prefect.engine
import pytest
import ray
import ray.cluster_utils
from prefect import flow, get_run_logger, task
from prefect.states import State
from prefect.testing.fixtures import hosted_orion_api, use_hosted_orion  # noqa: F401
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite
from prefect.testing.utilities import exceptions_equal
from ray.exceptions import TaskCancelledError

import tests
from prefect_ray import RayTaskRunner
from prefect_ray.context import remote_options


@pytest.fixture(scope="session")
def event_loop(request):
    """
    Redefine the event loop to support session/module-scoped fixtures;
    see https://github.com/pytest-dev/pytest-asyncio/issues/68
    When running on Windows we need to use a non-default loop for subprocess support.
    """
    if sys.platform == "win32" and sys.version_info >= (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    policy = asyncio.get_event_loop_policy()

    if sys.version_info < (3, 8) and sys.platform != "win32":
        from prefect.utilities.compat import ThreadedChildWatcher

        # Python < 3.8 does not use a `ThreadedChildWatcher` by default which can
        # lead to errors in tests as the previous default `SafeChildWatcher`  is not
        # compatible with threaded event loops.
        policy.set_child_watcher(ThreadedChildWatcher())

    loop = policy.new_event_loop()

    # configure asyncio logging to capture long running tasks
    asyncio_logger = logging.getLogger("asyncio")
    asyncio_logger.setLevel("WARNING")
    asyncio_logger.addHandler(logging.StreamHandler())
    loop.set_debug(True)
    loop.slow_callback_duration = 0.25

    try:
        yield loop
    finally:
        loop.close()

    # Workaround for failures in pytest_asyncio 0.17;
    # see https://github.com/pytest-dev/pytest-asyncio/issues/257
    policy.set_event_loop(loop)


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
def default_ray_task_runner():
    with warnings.catch_warnings():
        # Ray does not properly close resources and we do not want their warnings to
        # bubble into our test suite
        # https://github.com/ray-project/ray/pull/22419
        warnings.simplefilter("ignore", ResourceWarning)

        yield RayTaskRunner()


@pytest.fixture
def ray_task_runner_with_existing_cluster(
    machine_ray_instance, use_hosted_orion, hosted_orion_api  # noqa: F811
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
    inprocess_ray_cluster, use_hosted_orion, hosted_orion_api  # noqa: F811
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
def ray_task_runner_with_temporary_cluster(
    use_hosted_orion, hosted_orion_api  # noqa: F811
):
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


class TestRayTaskRunner(TaskRunnerStandardTestSuite):
    @pytest.fixture(
        params=[
            default_ray_task_runner,
            ray_task_runner_with_existing_cluster,
            ray_task_runner_with_inprocess_cluster,
            ray_task_runner_with_temporary_cluster,
        ]
    )
    def task_runner(self, request):
        yield request.getfixturevalue(
            request.param._pytestfixturefunction.name or request.param.__name__
        )

    def get_sleep_time(self) -> float:
        """
        Return an amount of time to sleep for concurrency tests.
        The RayTaskRunner is prone to flaking on concurrency tests.
        """
        return 5.0

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

        async def fake_orchestrate_task_run():
            raise exception

        test_key = uuid4()

        async with task_runner.start():
            await task_runner.submit(
                call=partial(fake_orchestrate_task_run),
                key=test_key,
            )

            state = await task_runner.wait(test_key, 5)
            assert state is not None, "wait timed out"
            assert isinstance(state, State), "wait should return a state"
            assert state.name == "Crashed"

    @pytest.mark.parametrize(
        "exceptions",
        [
            (KeyboardInterrupt(), TaskCancelledError),
            (ValueError("test"), ValueError),
        ],
    )
    async def test_exception_to_crashed_state_in_flow_run(
        self, exceptions, task_runner, monkeypatch
    ):

        (raised_exception, state_exception_type) = exceptions

        async def throws_exception_before_task_begins(
            task, task_run, parameters, wait_for, result_factory, settings, **kwds
        ):
            """
            Simulates an exception occurring while a remote task runner is attempting
            to unpickle and run a Prefect task.
            """
            raise raised_exception

        monkeypatch.setattr(
            prefect.engine, "begin_task_run", throws_exception_before_task_begins
        )

        @task()
        def test_task():
            logger = get_run_logger()
            logger.info("Ray should raise an exception before this task runs.")

        @flow(task_runner=task_runner)
        def test_flow():
            future = test_task.submit()
            future.wait(10)

        # ensure that the type of exception raised by the flow matches the type of
        # exception we expected the task runner to receive.
        with pytest.raises(state_exception_type) as exc:
            test_flow()
            # If Ray passes the same exception type back, it should pass
            # the equality check
            if type(raised_exception) == state_exception_type:
                assert exceptions_equal(raised_exception, exc)

    def test_flow_and_subflow_both_with_task_runner(self, task_runner, tmp_file):
        @task
        def some_task(text):
            tmp_file.write_text(text)

        @flow(task_runner=RayTaskRunner())
        def subflow():
            some_task.submit("a")
            some_task.submit("b")
            some_task.submit("c")

        @flow(task_runner=task_runner)
        def base_flow():
            subflow()
            time.sleep(self.get_sleep_time())
            some_task.submit("d")

        base_flow()
        assert tmp_file.read_text() == "d"

    def test_ray_options(self):
        @task
        def process(x):
            return x + 1

        @flow(task_runner=RayTaskRunner())
        def my_flow():
            # equivalent to setting @ray.remote(max_calls=1)
            with remote_options(max_calls=1):
                process.submit(42)

        my_flow()

    def test_dependencies(self):
        @task
        def a():
            time.sleep(self.get_sleep_time())

        b = c = d = e = a

        @flow(task_runner=RayTaskRunner())
        def flow_with_dependent_tasks():
            for _ in range(3):
                a_future = a.submit(wait_for=[])
                b_future = b.submit(wait_for=[a_future])

                c.submit(wait_for=[b_future])
                d.submit(wait_for=[b_future])
                e.submit(wait_for=[b_future])

        flow_with_dependent_tasks()

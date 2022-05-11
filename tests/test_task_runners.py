from uuid import uuid4

import pytest
from prefect.orion.schemas.core import TaskRun
from prefect.states import State
from prefect.task_runners import TaskConcurrencyType
from prefect.testing.standard_test_suites import TaskRunnerStandardTestSuite

# unable to get this to import automatically within pytest fixture
from .conftest import (
    default_ray_task_runner,
    ray_task_runner_with_existing_cluster,
    ray_task_runner_with_inprocess_cluster,
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

    def get_sleep_time(self) -> float:
        """
        Return an amount of time to sleep for concurrency tests.
        The RayTaskRunner is prone to flaking on concurrency tests.
        """
        return 5.0

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

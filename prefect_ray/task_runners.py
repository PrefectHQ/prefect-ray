"""
Interface and implementations of the Ray Task Runner.
[Task Runners](https://orion-docs.prefect.io/api-ref/prefect/task-runners/)
in Prefect are responsible for managing the execution of Prefect task runs.
Generally speaking, users are not expected to interact with
task runners outside of configuring and initializing them for a flow.

Example:
    ```python
    from prefect import flow, task
    from prefect.task_runners import SequentialTaskRunner
    from typing import List

    @task
    def say_hello(name):
        print(f"hello {name}")

    @task
    def say_goodbye(name):
        print(f"goodbye {name}")

    @flow(task_runner=SequentialTaskRunner())
    def greetings(names: List[str]):
        for name in names:
            say_hello(name)
            say_goodbye(name)
    ```

    Switching to a `RayTaskRunner`:
    ```python
    from prefect_ray.task_runners import RayTaskRunner
    flow.task_runner = RayTaskRunner()
    greetings(["arthur", "trillian", "ford", "marvin"])
    ```
    Output:
    ```
    hello arthur
    goodbye arthur
    hello trillian
    hello ford
    goodbye marvin
    hello marvin
    goodbye ford
    goodbye trillian
    ```
"""

from contextlib import AsyncExitStack
from typing import Any, Awaitable, Callable, Dict, Optional

import anyio
import ray
from prefect.futures import PrefectFuture
from prefect.orion.schemas.core import TaskRun
from prefect.orion.schemas.states import State
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R, TaskConcurrencyType
from prefect.utilities.asyncutils import A, sync_compatible


class RayTaskRunner(BaseTaskRunner):
    """
    A parallel task_runner that submits tasks to `ray`.
    By default, a temporary Ray cluster is created for the duration of the flow run.
    Alternatively, if you already have a `ray` instance running, you can provide
    the connection URL via the `address` kwarg.
    Args:
        address (string, optional): Address of a currently running `ray` instance; if
            one is not provided, a temporary instance will be created.
        init_kwargs (dict, optional): Additional kwargs to use when calling `ray.init`.
    Examples:
        Using a temporary local ray cluster:
        ```python
        from prefect import flow
        from prefect_ray.task_runners import RayTaskRunner

        @flow(task_runner=RayTaskRunner())
        def my_flow():
            ...
        ```
        Connecting to an existing ray instance:
        ```python
        RayTaskRunner(address="ray://192.0.2.255:8786")
        ```
    """

    def __init__(
        self,
        address: str = None,
        init_kwargs: dict = None,
    ):
        # Store settings
        self.address = address
        self.init_kwargs = init_kwargs.copy() if init_kwargs else {}

        self.init_kwargs.setdefault("namespace", "prefect")
        self.init_kwargs

        # Runtime attributes
        self._ray_refs: Dict[str, "ray.ObjectRef"] = {}

        super().__init__()

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return TaskConcurrencyType.PARALLEL

    async def submit(
        self,
        task_run: TaskRun,
        run_key: str,
        run_fn: Callable[..., Awaitable[State[R]]],
        run_kwargs: Dict[str, Any],
        asynchronous: A = True,
    ) -> PrefectFuture[R, A]:
        if not self._started:
            raise RuntimeError(
                "The task runner must be started before submitting work."
            )

        # Ray does not support the submission of async functions and we must create a
        # sync entrypoint
        self._ray_refs[run_key] = ray.remote(sync_compatible(run_fn)).remote(
            **run_kwargs
        )
        return PrefectFuture(
            task_run=task_run,
            task_runner=self,
            run_key=run_key,
            asynchronous=asynchronous,
        )

    async def wait(
        self,
        prefect_future: PrefectFuture,
        timeout: float = None,
    ) -> Optional[State]:
        ref = self._get_ray_ref(prefect_future)

        result = None

        with anyio.move_on_after(timeout):
            # We await the reference directly instead of using `ray.get` so we can
            # avoid blocking the event loop
            try:
                result = await ref
            except BaseException as exc:
                result = exception_to_crashed_state(exc)

        return result

    async def _start(self, exit_stack: AsyncExitStack):
        """
        Start the task runner and prep for context exit.

        - Creates a cluster if an external address is not set.
        - Creates a client to connect to the cluster.
        - Pushes a call to wait for all running futures to complete on exit.
        """
        if self.address:
            self.logger.info(
                f"Connecting to an existing Ray instance at {self.address}"
            )
            init_args = (self.address,)
        else:
            self.logger.info("Creating a local Ray instance")
            init_args = ()

        context = ray.init(*init_args, **self.init_kwargs)
        dashboard_url = getattr(context, "dashboard_url", None)
        exit_stack.push(context)

        # Display some information about the cluster
        nodes = ray.nodes()
        living_nodes = [node for node in nodes if node.get("alive")]
        self.logger.info(f"Using Ray cluster with {len(living_nodes)} nodes.")

        if dashboard_url:
            self.logger.info(
                f"The Ray UI is available at {dashboard_url}",
            )

    async def _shutdown_ray(self):
        """
        Shuts down the cluster.
        """
        self.logger.debug("Shutting down Ray cluster...")
        ray.shutdown()

    def _get_ray_ref(self, prefect_future: PrefectFuture) -> "ray.ObjectRef":
        """
        Retrieve the ray object reference corresponding to a prefect future.
        """
        return self._ray_refs[prefect_future.run_key]

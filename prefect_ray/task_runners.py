from contextlib import AsyncExitStack
from typing import Any, Awaitable, Callable, Dict, Optional
from uuid import UUID

import anyio
import ray
from prefect.futures import PrefectFuture
from prefect.orion.schemas.core import TaskRun
from prefect.orion.schemas.states import State
from prefect.states import exception_to_crashed_state
from prefect.task_runners import BaseTaskRunner, R
from prefect.utilities.asyncio import A, sync_compatible
from prefect.task_runners import TaskConcurrencyType


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
        >>> from prefect import flow
        >>> from prefect.task_runners import RayTaskRunner
        >>> @flow(task_runner=RayTaskRunner)
        Connecting to an existing ray instance:
        >>> RayTaskRunner(address="ray://192.0.2.255:8786")
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
        self._ray_refs: Dict[UUID, "ray.ObjectRef"] = {}

        super().__init__()

    @property
    def concurrency_type(self) -> TaskConcurrencyType:
        return TaskConcurrencyType.PARALLEL

    async def submit(
        self,
        task_run: TaskRun,
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
        self._ray_refs[task_run.id] = ray.remote(sync_compatible(run_fn)).remote(
            **run_kwargs
        )
        return PrefectFuture(
            task_run=task_run, task_runner=self, asynchronous=asynchronous
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

    @property
    def _ray(self) -> "ray":
        """
        Delayed import of `ray` allowing configuration of the task runner
        without the extra installed and improves `prefect` import times.
        """
        global ray

        if ray is None:
            try:
                import ray
            except ImportError as exc:
                raise RuntimeError(
                    "Using the `RayTaskRunner` requires `ray` to be installed."
                ) from exc

        return ray

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

        # In ray < 1.11.0, connecting to an out-of-process cluster (e.g. ray://ip)
        # returns a `ClientContext` otherwise it returns a `dict`.
        # In ray >= 1.11.0, a context is always returned.
        context_or_metadata = self._ray.init(*init_args, **self.init_kwargs)
        if hasattr(context_or_metadata, "__enter__"):
            context = context_or_metadata
            metadata = getattr(context, "address_info", {})
            dashboard_url = getattr(context, "dashboard_url", None)
        else:
            context = None
            metadata = context_or_metadata
            dashboard_url = metadata.get("webui_url")

        if context:
            exit_stack.push(context)
        else:
            # If not given a context, call shutdown manually at exit
            exit_stack.push_async_callback(self._shutdown_ray)

        # Display some information about the cluster
        nodes = ray.nodes()
        living_nodes = [node for node in nodes if node.get("alive")]
        self.logger.info(f"Using Ray cluster with {len(living_nodes)} nodes.")

        if dashboard_url:
            self.logger.info(
                f"The Ray UI is available at {dashboard_url}",
            )

    async def _shutdown_ray(self):
        self.logger.debug("Shutting down Ray cluster...")
        self._ray.shutdown()

    def _get_ray_ref(self, prefect_future: PrefectFuture) -> "ray.ObjectRef":
        """
        Retrieve the ray object reference corresponding to a prefect future.
        """
        return self._ray_refs[prefect_future.run_id]
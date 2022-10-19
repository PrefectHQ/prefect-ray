"""
Contexts to manage Ray clusters and tasks.
"""

from contextlib import contextmanager
from typing import Any, Dict

from prefect.context import ContextModel, ContextVar
from pydantic import Field


class ResourcesContext(ContextModel):
    """
    The context for Ray resources management.

    Attributes:
        current_resources: A set of current resources in the context.
    """

    current_resources: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def get(cls) -> "ResourcesContext":
        """Return an empty `ResourcesContext` instead of `None` if no context exists."""
        return cls.__var__.get(ResourcesContext())

    __var__ = ContextVar("resources")


@contextmanager
def resources(**new_resources: Dict[str, Any]) -> Dict[str, Any]:
    """
    Context manager to add resources to flow and task run calls.
    Resources are always combined with any existing resources.

    Yields:
        The current set of resources.

    Examples:
        Use 4 CPUs and 2 GPUs for the `process` task:
        ```python
        from prefect import flow, task
        from prefect_ray.task_runners import RayTaskRunner
        from prefect_ray.context import resources

        @task
        def process(x):
            return x + 1

        @flow(task_runner=RayTaskRunner())
        def my_flow():
            # equivalent to setting @ray.remote(num_cpus=4, num_gpus=2)
            with resources(num_cpus=4, num_gpus=2):
                process.submit(42)
        ```
    """
    current_resources = ResourcesContext.get().current_resources
    new_resources.update(**current_resources)
    with ResourcesContext(current_resources=new_resources):
        yield

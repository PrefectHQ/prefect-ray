# prefect-ray

## Welcome!

Prefect integrations with the [Ray](https://www.ray.io/) execution framework, a flexible distributed computing framework for Python.

Provides a `RayTaskRunner` that enables flows to run tasks requiring parallel execution using Ray.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda, or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-ray` with `pip`:

```bash
pip install prefect-ray
```

## Running tasks on Ray

The `RayTaskRunner` is a [Prefect task runner](https://orion-docs.prefect.io/concepts/task-runners/) that submits tasks to [Ray](https://www.ray.io/) for parallel execution. 

By default, a temporary Ray instance is created for the duration of the flow run.

For example, this flow says hello and goodbye in parallel.
```python
from prefect import flow, task
from prefect_ray.task_runners import RayTaskRunner
from typing import List

@task
def say_hello(name):
    print(f"hello {name}")

@task
def say_goodbye(name):
    print(f"goodbye {name}")

@flow(task_runner=RayTaskRunner())
def greetings(names: List[str]):
    for name in names:
        say_hello(name)
        say_goodbye(name)


greetings(["arthur", "trillian", "ford", "marvin"])

# truncated output
...
goodbye trillian
goodbye arthur
hello trillian
hello ford
hello marvin
hello arthur
goodbye ford
goodbye marvin
...
```

If you already have a Ray instance running, you can provide the connection URL via an `address` argument.

To configure your flow to use the `RayTaskRunner`:

1. Make sure the `prefect-ray` collection is installed as described earlier: `pip install prefect-ray`.
2. In your flow code, import `RayTaskRunner` from `prefect_ray.task_runners`.
3. Assign it as the task runner when the flow is defined using the `task_runner=RayTaskRunner` argument.

For example, this flow uses the `RayTaskRunner` with a local, temporary Ray instance created by Prefect at flow run time.

```python
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

@flow(task_runner=RayTaskRunner())
def my_flow():
    ... 
```

This flow uses the `RayTaskRunner` configured to access an existing Ray instance at `ray://192.0.2.255:8786`.

```python
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

@flow(task_runner=RayTaskRunner(address="ray://192.0.2.255:8786"))
def my_flow():
    ... 
```

`RayTaskRunner` accepts the following optional parameters:

| Parameter | Description |
| --- | --- |
| address | Address of a currently running Ray instance, starting with the [ray://](https://docs.ray.io/en/master/cluster/ray-client.html) URI. |
| init_kwargs | Additional kwargs to use when calling `ray.init`. |

Note that Ray Client uses the [ray://](https://docs.ray.io/en/master/cluster/ray-client.html) URI to indicate the address of a Ray instance. If you don't provide the `address` of a Ray instance, Prefect creates a temporary instance automatically.

!!! warning "Ray environment limitations"
    While we're excited about adding support for parallel task execution via Ray to Prefect, there are some inherent limitations with Ray you should be aware of:
    
    Ray currently does not support Python 3.10.

    Ray support for non-x86/64 architectures such as ARM/M1 processors with installation from `pip` alone and will be skipped during installation of Prefect. It is possible to manually install the blocking component with `conda`. See the [Ray documentation](https://docs.ray.io/en/latest/ray-overview/installation.html#m1-mac-apple-silicon-support) for instructions.

    See the [Ray installation documentation](https://docs.ray.io/en/latest/ray-overview/installation.html) for further compatibility information.


## Resources

If you encounter and bugs while using `prefect-ray`, feel free to open an issue in the [prefect-ray](https://github.com/PrefectHQ/prefect-ray) repository.

If you have any questions or issues while using `prefect-ray`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-ray` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-ray.git

cd prefect-ray/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```

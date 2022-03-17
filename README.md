# prefect-ray

## Welcome!

Prefect integrations with the Ray execution framework.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-ray` with `pip`:

```bash
pip install prefect-ray
```

### Write and run a flow

```python
from prefect import flow
from prefect_ray.tasks import (
    goodbye_prefect_ray,
    hello_prefect_ray,
)


@flow
def example_flow():
    hello_prefect_ray
    goodbye_prefect_ray

example_flow()
```

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

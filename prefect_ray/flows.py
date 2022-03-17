"""This is an example flows module"""
from prefect import flow

from prefect_ray.tasks import (
    goodbye_prefect_ray,
    hello_prefect_ray,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_ray)
    print(goodbye_prefect_ray)

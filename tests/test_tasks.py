from prefect import flow

from prefect_ray.tasks import (
    goodbye_prefect_ray,
    hello_prefect_ray,
)


def test_hello_prefect_ray():
    @flow
    def test_flow():
        return hello_prefect_ray()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-ray!"


def goodbye_hello_prefect_ray():
    @flow
    def test_flow():
        return goodbye_prefect_ray()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-ray!"

from prefect_ray.context import ResourcesContext, resources


def test_resources_context():
    input_resources = {"num_cpus": 4}
    current_resources = ResourcesContext(
        current_resources=input_resources
    ).current_resources
    assert current_resources == input_resources


def test_resources_context_get():
    current_resources = ResourcesContext.get().current_resources
    assert current_resources == {}


def test_resources():
    with resources(num_cpus=4, num_gpus=None):
        current_resources = ResourcesContext.get().current_resources
        assert current_resources == {"num_cpus": 4, "num_gpus": None}


def test_resources_empty():
    with resources():
        current_resources = ResourcesContext.get().current_resources
        assert current_resources == {}

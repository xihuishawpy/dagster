from memoized_development.solids.solid_utils import get_hash_for_file

from dagster import solid


@solid(version=get_hash_for_file(__file__), config_schema={"dog_breed": str})
def emit_dog(context):
    return context.solid_config["dog_breed"]

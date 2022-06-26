import requests

from dagster import asset

# start_example


@asset(config_schema={"api_endpoint": str})
def my_configurable_asset(context):
    api_endpoint = context.op_config["api_endpoint"]
    return requests.get(f"{api_endpoint}/data").json()


# end_example

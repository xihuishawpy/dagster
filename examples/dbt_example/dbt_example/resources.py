from unittest.mock import MagicMock

import sqlalchemy

from dagster import resource


@resource(config_schema={"db_url": str})
def postgres(context):
    return sqlalchemy.create_engine(context.resource_config["db_url"])


@resource(config_schema={"token": str})
def mock_slack_resource(_context):
    return MagicMock()

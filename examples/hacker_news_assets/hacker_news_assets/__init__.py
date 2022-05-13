from hacker_news_assets.activity_analytics import activity_analytics_definitions
from hacker_news_assets.core import core_definitions
from hacker_news_assets.recommender import recommender_definitions
from hacker_news_assets.resources import RESOURCES_LOCAL, RESOURCES_PROD, RESOURCES_STAGING

from dagster import repository, with_resources

from .sensors.slack_on_failure_sensor import make_slack_on_failure_sensor


@repository
def prod():
    return with_resources(
        [
            *core_definitions,
            *recommender_definitions,
            *activity_analytics_definitions,
            make_slack_on_failure_sensor(base_url="my_dagit_url"),
        ],
        RESOURCES_PROD,
    )


@repository
def staging():
    return with_resources(
        [
            *core_definitions,
            *recommender_definitions,
            *activity_analytics_definitions,
            make_slack_on_failure_sensor(base_url="my_dagit_url"),
        ],
        RESOURCES_STAGING,
    )


@repository
def local():
    return with_resources(
        [
            *core_definitions,
            *recommender_definitions,
            *activity_analytics_definitions,
        ],
        RESOURCES_LOCAL,
    )

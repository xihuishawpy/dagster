from pandas import DataFrame

from dagster import In, Out, job, op, repository

from .mylib import s3_io_manager, snowflake_io_manager, train_recommender_model


@op(
    ins={"raw_users": In(root_manager_key="warehouse")},
    out={"users": Out(io_manager_key="warehouse")},
)
def build_users(raw_users: DataFrame) -> DataFrame:
    return raw_users.dropna()


@op(out={"users_recommender_model": Out(io_manager_key="object_store")})
def build_user_recommender_model(users: DataFrame):
    return train_recommender_model(users)


@job(resource_defs={"warehouse": snowflake_io_manager, "object_store": s3_io_manager})
def users_recommender_job():
    build_user_recommender_model(build_users())


@repository
def repo():
    return [users_recommender_job]

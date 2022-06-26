# start_marker

from typing import List

from dagster import job, op


@op
def return_one() -> int:
    return 1


@op
def sum_fan_in(nums: List[int]) -> int:
    return sum(nums)


@job
def fan_in():
    fan_outs = [return_one.alias(f"return_one_{i}")() for i in range(10)]
    sum_fan_in(fan_outs)


# end_marker

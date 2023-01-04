from functools import reduce
from typing import List

from pyspark.sql import Column
from pyspark.sql import functions as f


def array_contains_all(col: Column, values: List[str]) -> Column:
    if not values:
        return f.lit(True)

    return reduce(
        lambda first_condition, second_condition: (first_condition) & (second_condition),  # noqa
        [f.array_contains(col, value) for value in values],
    )


def array_contains_any(col: Column, values: List[str]) -> Column:
    if not values:
        return f.lit(True)

    return reduce(
        lambda first_condition, second_condition: (first_condition) | (second_condition),  # noqa
        [f.array_contains(col, value) for value in values],
    )


def column(name: str, col: Column) -> Column:
    return col.alias(name)


def most_common(name: str, *columns: Column) -> Column:
    return f.max(f.struct(*columns)).alias(name)

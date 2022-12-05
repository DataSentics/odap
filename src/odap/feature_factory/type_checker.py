from numbers import Number
from datetime import datetime
from odap.feature_factory.exceptions import WrongFillnaValueTypeError


def check_fillna_valid(dtype: str, value, feature_name: str):
    if value is None:
        return

    if (
        (is_feature_bool(dtype) and not is_value_bool(value))  # pylint: disable=too-many-boolean-expressions
        or (is_feature_numeric(dtype) and not is_value_numeric(value))
        or (is_feature_string(dtype) and not is_value_string(value))
        or (is_feature_datetime(dtype) and not is_value_datetime(value))
        or (is_feature_array(dtype) and not is_value_array(value))
    ):
        raise WrongFillnaValueTypeError(value, feature_name, dtype)


def is_value_bool(value) -> bool:
    return isinstance(value, bool)


def is_feature_bool(dtype: str) -> bool:
    return dtype == "boolean"


def is_value_numeric(value) -> bool:
    return isinstance(value, Number)


def is_feature_numeric(dtype: str) -> bool:
    return dtype in ["byte", "short", "integer", "long", "float", "double"] or dtype.startswith("decimal")


def is_value_string(value) -> bool:
    return isinstance(value, str)


def is_feature_string(dtype: str) -> bool:
    return dtype == "string"


def is_value_datetime(value) -> bool:
    return isinstance(value, datetime)


def is_feature_datetime(dtype: str) -> bool:
    return dtype in ["date", "timestamp"]


def is_value_array(value) -> bool:
    return isinstance(value, list)


def is_feature_array(dtype: str) -> bool:
    return dtype.startswith("array")

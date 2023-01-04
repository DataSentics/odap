from typing import Any, Dict, List, Optional
import re

import pyspark.sql.types as t
from pyspark.sql import DataFrame

from odap.feature_factory import const
from odap.common.exceptions import NotebookException


RawMetadataType = Dict[str, Any]
FeatureMetadataType = Dict[str, Any]
FeaturesMetadataType = List[FeatureMetadataType]


types_normalization_map = {
    t.StringType().simpleString(): "string",
    t.BooleanType().simpleString(): "boolean",
    t.ByteType().simpleString(): "byte",
    t.ShortType().simpleString(): "short",
    t.IntegerType().simpleString(): "integer",
    t.LongType().simpleString(): "long",
    t.FloatType().simpleString(): "float",
    t.DoubleType().simpleString(): "double",
    t.TimestampType().simpleString(): "timestamp",
    t.DateType().simpleString(): "date",
}

variable_types_map = {
    "string": "categorical",
    "boolean": "binary",
    "byte": "numerical",
    "short": "numerical",
    "integer": "numerical",
    "long": "numerical",
    "float": "numerical",
    "double": "numerical",
}


def get_metadata_pk_columns() -> List[t.StructField]:
    return [t.StructField(const.FEATURE, t.StringType(), False)]


def get_metadata_columns() -> List[t.StructField]:
    return [
        t.StructField(const.DESCRIPTION, t.StringType(), True),
        t.StructField(const.EXTRA, t.MapType(t.StringType(), t.StringType()), True),
        t.StructField(const.FEATURE_TEMPLATE, t.StringType(), True),
        t.StructField(const.DESCRIPTION_TEMPLATE, t.StringType(), True),
        t.StructField(const.CATEGORY, t.StringType(), True),
        t.StructField(const.OWNER, t.StringType(), True),
        t.StructField(const.TAGS, t.ArrayType(t.StringType()), True),
        t.StructField(const.START_DATE, t.TimestampType(), True),
        t.StructField(const.FREQUENCY, t.StringType(), True),
        t.StructField(const.LAST_COMPUTE_DATE, t.TimestampType(), True),
        t.StructField(const.DTYPE, t.StringType(), True),
        t.StructField(const.VARIABLE_TYPE, t.StringType(), True),
        t.StructField(const.FILLNA_VALUE, t.StringType(), True),
        t.StructField(const.FILLNA_VALUE_TYPE, t.StringType(), True),
        t.StructField(const.NOTEBOOK_NAME, t.StringType(), True),
        t.StructField(const.NOTEBOOK_ABSOLUTE_PATH, t.StringType(), True),
        t.StructField(const.NOTEBOOK_RELATIVE_PATH, t.StringType(), True),
        t.StructField(const.TABLE, t.StringType(), True),
        t.StructField(const.BACKEND, t.StringType(), True),
    ]


def get_metadata_schema() -> t.StructType:
    return t.StructType(get_metadata_pk_columns() + get_metadata_columns())


def get_feature_field(feature_df: DataFrame, feature_name: str, feature_path: str) -> t.StructField:
    for field in feature_df.schema.fields:
        if field.name == feature_name:
            return field

    raise NotebookException(f"Feature {feature_name} from metadata isn't present in it's DataFrame!", path=feature_path)


def normalize_dtype(dtype: str) -> str:
    for key, val in types_normalization_map.items():
        dtype = re.sub(f"\\b{key}\\b", val, dtype)

    return dtype


def get_feature_dtype(feature_field: t.StructField) -> str:
    dtype = feature_field.dataType.simpleString()
    return normalize_dtype(dtype)


def get_variable_type(dtype: str) -> Optional[str]:
    if dtype.startswith("decimal"):
        return "numerical"

    if dtype.startswith("array"):
        return "array"

    return variable_types_map.get(dtype)

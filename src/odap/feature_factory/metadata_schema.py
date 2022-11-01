from typing import Any, Dict, List, Optional
import re

import pyspark.sql.types as t
from pyspark.sql import DataFrame
from odap.feature_factory.exceptions import NotebookException

FEATURE = "feature"
DESCRIPTION = "description"
EXTRA = "extra"
FEATURE_TEMPLATE = "feature_template"
DESCRIPTION_TEMPLATE = "description_template"
CATEGORY = "category"
OWNER = "owner"
TAGS = "tags"
START_DATE = "start_date"
FREQUENCY = "frequency"
LAST_COMPUTE_DATE = "last_compute_date"
DTYPE = "dtype"
VARIABLE_TYPE = "variable_type"
FILLNA_VALUE = "fillna_value"
FILLNA_VALUE_TYPE = "fillna_value_type"
NOTEBOOK_NAME = "notebook_name"
NOTEBOOK_ABSOLUTE_PATH = "notebook_absolute_path"
NOTEBOOK_RELATIVE_PATH = "notebook_relative_path"
LOCATION = "location"
BACKEND = "backend"

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


def get_metadata_schema():
    return t.StructType(
        [
            t.StructField(FEATURE, t.StringType(), False),
            t.StructField(DESCRIPTION, t.StringType(), True),
            t.StructField(EXTRA, t.MapType(t.StringType(), t.StringType()), True),
            t.StructField(FEATURE_TEMPLATE, t.StringType(), True),
            t.StructField(DESCRIPTION_TEMPLATE, t.StringType(), True),
            t.StructField(CATEGORY, t.StringType(), True),
            t.StructField(OWNER, t.StringType(), True),
            t.StructField(TAGS, t.ArrayType(t.StringType()), True),
            t.StructField(START_DATE, t.TimestampType(), True),
            t.StructField(FREQUENCY, t.StringType(), True),
            t.StructField(LAST_COMPUTE_DATE, t.TimestampType(), True),
            t.StructField(DTYPE, t.StringType(), True),
            t.StructField(VARIABLE_TYPE, t.StringType(), True),
            t.StructField(FILLNA_VALUE, t.StringType(), True),
            t.StructField(FILLNA_VALUE_TYPE, t.StringType(), True),
            t.StructField(NOTEBOOK_NAME, t.StringType(), True),
            t.StructField(NOTEBOOK_ABSOLUTE_PATH, t.StringType(), True),
            t.StructField(NOTEBOOK_RELATIVE_PATH, t.StringType(), True),
            t.StructField(LOCATION, t.StringType(), True),
            t.StructField(BACKEND, t.StringType(), True),
        ]
    )


def get_feature_field(feature_df: DataFrame, feature_name: str, feature_path: str) -> t.StructField:
    for field in feature_df.schema.fields:
        if field.name == feature_name:
            return field

    raise NotebookException(f"Feature {feature_name} from metadata isn't present in it's DataFrame!", feature_path)


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

    return variable_types_map.get(dtype)

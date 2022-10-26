from typing import Any, Dict, List
import re

import pyspark.sql.types as t
from pyspark.sql import DataFrame
from odap.feature_factory.exceptions import (
    FeatureNotPresentInDataframeException,
)

FEATURE = "feature"
FEATURE_TEMPLATE = "feature_template"
DESCRIPTION = "description"
DESCRIPTION_TEMPLATE = "description_template"
DTYPE = "dtype"
CATEGORY = "category"
TAGS = "tags"
NOTEBOOK_NAME = "notebook_name"
NOTEBOOK_ABSOLUTE_PATH = "notebook_absolute_path"
NOTEBOOK_RELATIVE_PATH = "notebook_relative_path"

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


def get_metadata_schema():
    return t.StructType(
        [
            t.StructField(FEATURE, t.StringType(), False),
            t.StructField(DESCRIPTION, t.StringType(), True),
            t.StructField(FEATURE_TEMPLATE, t.StringType(), True),
            t.StructField(DESCRIPTION_TEMPLATE, t.StringType(), True),
            t.StructField(DTYPE, t.StringType(), True),
            t.StructField(CATEGORY, t.StringType(), True),
            t.StructField(TAGS, t.ArrayType(t.StringType()), True),
            t.StructField(NOTEBOOK_NAME, t.StringType(), True),
            t.StructField(NOTEBOOK_ABSOLUTE_PATH, t.StringType(), True),
            t.StructField(NOTEBOOK_RELATIVE_PATH, t.StringType(), True),
        ]
    )


def get_feature_field(feature_df: DataFrame, feature_name: str) -> t.StructField:
    for field in feature_df.schema.fields:
        if field.name == feature_name:
            return field

    raise FeatureNotPresentInDataframeException(
        f"Feature {feature_name} from metadata isn't present in it's DataFrame!"
    )


def normalize_dtype(dtype: str) -> str:
    for key, val in types_normalization_map.items():
        dtype = re.sub(f"\\b{key}\\b", val, dtype)

    return dtype


def get_feature_dtype(feature_field: t.StructField) -> str:
    dtype = feature_field.dataType.simpleString()
    return normalize_dtype(dtype)

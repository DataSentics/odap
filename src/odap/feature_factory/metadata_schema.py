from typing import Any, Dict, List

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

FeatureMetadataType = Dict[str, Any]
FeaturesMetadataType = List[FeatureMetadataType]


def get_metadata_schema():
    return t.StructType(
        [
            t.StructField(FEATURE, t.StringType(), False),
            t.StructField(DESCRIPTION, t.StringType(), True),
            t.StructField(FEATURE_TEMPLATE, t.StringType(), True),
            t.StructField(DESCRIPTION_TEMPLATE, t.StringType(), True),
            t.StructField(DTYPE, t.StringType(), True),
            t.StructField("category", t.StringType(), True),
            t.StructField("tags", t.ArrayType(t.StringType()), True),
        ]
    )


def get_array_columns() -> List[str]:
    fields = get_metadata_schema()
    return [field.name for field in fields if isinstance(field.dataType, t.ArrayType)]


def get_feature_field(feature_df: DataFrame, feature_name: str) -> t.StructField:
    for field in feature_df.schema.fields:
        if field.name == feature_name:
            return field

    raise FeatureNotPresentInDataframeException(
        f"Feature {feature_name} from metadata isn't present in it's DataFrame!"
    )


def get_feature_dtype(feature_field: t.StructField) -> str:
    return feature_field.dataType.simpleString()

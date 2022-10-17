from typing import Any, Dict, List

import re
import json
import pyspark.sql.types as t
from pyspark.sql import DataFrame, SparkSession
from odap.feature_factory.exceptions import MissingMetadataException

METADATA_HEADER = "# Metadata"
SQL_MAGIC_DIVIDER = "-- MAGIC "
PYTHON_MAGIC_DIVIDER = "# MAGIC "
FEATURE_METADATA_COLUMN = "feature"


def get_metadata_schema():
    return t.StructType(
        [
            t.StructField(FEATURE_METADATA_COLUMN, t.StringType(), False),
            t.StructField("description", t.StringType(), True),
            t.StructField("tags", t.ArrayType(t.StringType()), True),
        ]
    )


def split_feature_metadata(feature: str) -> List[str]:
    if SQL_MAGIC_DIVIDER in feature:
        return feature.split(SQL_MAGIC_DIVIDER)

    return feature.split(PYTHON_MAGIC_DIVIDER)


def parse_value(value: str):
    if value.startswith("[") and value.endswith("]"):
        value = json.loads(value)
    return value


def parse_feature(feature: str) -> Dict[str, Any]:
    parsed_feature = {}

    feature_lines = split_feature_metadata(feature)

    parsed_feature["feature"] = feature_lines.pop(0).strip()

    for line in feature_lines:
        searched = re.search(r"\s*-\s*(.*):\s*(.*)", line)

        if not searched:
            continue

        attr = searched.group(1)
        value = searched.group(2)

        parsed_feature[attr] = parse_value(value)

    return parsed_feature


def parse_metadata(metadata: str) -> List[Dict[str, Any]]:
    parsed_metadata = []
    features = metadata.split("##")

    for feature in features[1:]:
        parsed_metadata.append(parse_feature(feature))

    return parsed_metadata


def extract_metadata_from_cells(cells: List[str], feature_path: str) -> List[Dict[str, Any]]:
    metadata = None
    for current_cell in cells:
        if METADATA_HEADER in current_cell:
            metadata = parse_metadata(current_cell)
            cells.remove(current_cell)
            break

    if not metadata:
        raise MissingMetadataException(f"Metadata not provided for feature {feature_path}")

    return metadata


def create_metadata_dataframe(metadata: Dict[str, Any]) -> DataFrame:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    return spark.createDataFrame(data=metadata, schema=get_metadata_schema())

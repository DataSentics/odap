from typing import Any, Dict, List, Union

import re

from pyspark.sql import DataFrame, SparkSession
from odap.common.utils import get_notebook_name, get_relative_path
from odap.feature_factory.exceptions import (
    MetadataParsingException,
    MissingMetadataException,
)
from odap.feature_factory.templates import resolve_metadata_template
from odap.feature_factory.metadata_schema import (
    DTYPE,
    FEATURE,
    NOTEBOOK_NAME,
    NOTEBOOK_ABSOLUTE_PATH,
    NOTEBOOK_RELATIVE_PATH,
    FeatureMetadataType,
    FeaturesMetadataType,
    get_array_columns,
    get_feature_dtype,
    get_feature_field,
    get_metadata_schema,
)

METADATA_HEADER = "# Metadata"
FEATURES_HEADER = "## Features"

SQL_MAGIC_DIVIDER = "-- MAGIC "
PYTHON_MAGIC_DIVIDER = "# MAGIC "

FEATURE_REGEX = r"^- `([a-zA-Z0-9_{}]*)`$"
METADATA_REGEX = r"^  - (.*): `(.*)`"
GLOBAL_METADATA_REGEX = r"^- (.*): `(.*)`"


def remove_magic_dividers(metadata: str) -> str:
    if SQL_MAGIC_DIVIDER in metadata:
        return metadata.replace(SQL_MAGIC_DIVIDER, "")

    return metadata.replace(PYTHON_MAGIC_DIVIDER, "")


def split_to_lines(metadata: str) -> List[str]:
    return metadata.split("\n")


def parse_value(metadata_name: str, metadata_value: str) -> Union[str, List[str]]:
    array_columns = get_array_columns()

    if metadata_name in array_columns:
        return [value.strip() for value in metadata_value.split(",")]

    return metadata_value


def get_metadata_dict_from_line(line: str, feature_path: str, regex=METADATA_REGEX) -> Dict[str, Union[str, List[str]]]:
    searched = re.search(regex, line)

    if not searched:
        raise MetadataParsingException(f"Syntax error at '{line}'. Feature path: {feature_path}")

    attr = searched.group(1)

    if attr not in get_metadata_schema().fieldNames():
        raise MetadataParsingException(f"{attr} is not a supported metadata field. Feature path: {feature_path}")

    value = searched.group(2)

    return {attr: parse_value(attr, value)}


def get_feature_name(feature_name_line: str, feature_path: str) -> str:
    searched = re.search(FEATURE_REGEX, feature_name_line)

    if not searched:
        raise MetadataParsingException(f"Syntax error at '{feature_name_line}'. Feature path: {feature_path}")

    return searched.group(1)


def parse_feature(feature: str, feature_path: str) -> FeatureMetadataType:
    parsed_feature = {}

    feature_lines = split_to_lines(feature)

    parsed_feature[FEATURE] = get_feature_name(feature_lines.pop(0), feature_path)

    for line in feature_lines:
        if line:
            parsed_feature.update(get_metadata_dict_from_line(line, feature_path))

    return parsed_feature


def set_notebook_paths(feature_path: str, global_metadata_dict: FeatureMetadataType):
    global_metadata_dict[NOTEBOOK_NAME] = get_notebook_name(feature_path)
    global_metadata_dict[NOTEBOOK_ABSOLUTE_PATH] = feature_path
    global_metadata_dict[NOTEBOOK_RELATIVE_PATH] = get_relative_path(feature_path)


def extract_global_metadata(metadata: str, feature_path: str, global_metadata_dict: FeatureMetadataType) -> str:
    if not FEATURES_HEADER in metadata:
        raise MetadataParsingException(f"## Features section is missing in metadata for feature {feature_path}")

    global_metadata, features_metadata_string = metadata.split(FEATURES_HEADER)

    for line in split_to_lines(global_metadata):
        if line:
            global_metadata_dict.update(get_metadata_dict_from_line(line, feature_path, regex=GLOBAL_METADATA_REGEX))

    set_notebook_paths(feature_path, global_metadata_dict)

    return features_metadata_string


def set_features_dtype(feature_df: DataFrame, parsed_features: FeaturesMetadataType):
    for parsed_feature in parsed_features:
        feature_field = get_feature_field(feature_df, parsed_feature[FEATURE])
        parsed_feature[DTYPE] = get_feature_dtype(feature_field)


def parse_metadata(metadata: str, feature_path: str, feature_df: DataFrame) -> FeaturesMetadataType:
    parsed_metadata = []
    global_metadata = {}

    features_metadata_string = extract_global_metadata(metadata, feature_path, global_metadata)

    for feature in features_metadata_string.split("\n- "):
        if not feature:
            continue

        parsed_feature_metadata = parse_feature(f"- {feature}", feature_path)

        parsed_feature_metadata.update(global_metadata)

        parsed_metadata.extend(resolve_metadata_template(feature_df, parsed_feature_metadata))

    set_features_dtype(feature_df, parsed_metadata)

    return parsed_metadata


def remove_metadata_header(metadata: str) -> str:
    return metadata.split(METADATA_HEADER)[1]


def extract_metadata_string_from_cells(cells: List[str], feature_path: str) -> str:
    for current_cell in cells[:]:
        if METADATA_HEADER in current_cell:
            metadata = remove_magic_dividers(current_cell)
            metadata = remove_metadata_header(metadata)

            cells.remove(current_cell)

            return metadata

    raise MissingMetadataException(f"Metadata not provided for feature {feature_path}")


def create_metadata_dataframe(metadata: Dict[str, Any]) -> DataFrame:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    return spark.createDataFrame(data=metadata, schema=get_metadata_schema())

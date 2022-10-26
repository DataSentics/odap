from typing import Any, Dict, List

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
    RawMetadataType,
    get_feature_dtype,
    get_feature_field,
    get_metadata_schema,
)

METADATA_HEADER = "metadata = {"

SQL_MAGIC_DIVIDER = "-- MAGIC "
PYTHON_MAGIC_DIVIDER = "# MAGIC "


def remove_magic_dividers(metadata: str) -> str:
    if SQL_MAGIC_DIVIDER in metadata:
        return metadata.replace(SQL_MAGIC_DIVIDER, "")

    return metadata.replace(PYTHON_MAGIC_DIVIDER, "")


def set_notebook_paths(feature_path: str, global_metadata_dict: FeatureMetadataType):
    global_metadata_dict[NOTEBOOK_NAME] = get_notebook_name(feature_path)
    global_metadata_dict[NOTEBOOK_ABSOLUTE_PATH] = feature_path
    global_metadata_dict[NOTEBOOK_RELATIVE_PATH] = get_relative_path(feature_path)


def set_features_dtype(feature_df: DataFrame, parsed_features: FeaturesMetadataType):
    for parsed_feature in parsed_features:
        feature_field = get_feature_field(feature_df, parsed_feature[FEATURE])
        parsed_feature[DTYPE] = get_feature_dtype(feature_field)


def get_features_from_raw_metadata(raw_metadata: RawMetadataType, feature_path: str) -> FeaturesMetadataType:
    raw_features = raw_metadata.pop("features", None)

    if not raw_features:
        MetadataParsingException(f"No features provided in metadata. Feature: '{feature_path}'")

    for feature_name, value_dict in raw_features.items():
        value_dict[FEATURE] = feature_name

    return list(raw_features.values())


def check_metadata(metadata: FeatureMetadataType, feature_path: str):
    for field in metadata:
        if field not in get_metadata_schema().fieldNames():
            raise MetadataParsingException(f"{field} is not a supported metadata field. Feature path: {feature_path}")

    return metadata


def get_global_metadata(raw_metadata: RawMetadataType, feature_path: str) -> FeatureMetadataType:
    check_metadata(raw_metadata, feature_path)

    set_notebook_paths(feature_path, global_metadata_dict=raw_metadata)

    return raw_metadata


def resolve_metadata(raw_metadata: RawMetadataType, feature_path: str, feature_df: DataFrame) -> FeaturesMetadataType:
    parsed_metadata = []

    features = get_features_from_raw_metadata(raw_metadata, feature_path)
    global_metadata = get_global_metadata(raw_metadata, feature_path)

    for feature_metadata in features:
        check_metadata(feature_metadata, feature_path)

        feature_metadata.update(global_metadata)

        parsed_metadata.extend(resolve_metadata_template(feature_df, feature_metadata))

    set_features_dtype(feature_df, parsed_metadata)

    return parsed_metadata


def get_metadata_dict(cell: str, feature_path: str):
    cell = cell[cell.find(METADATA_HEADER) :]

    exec(cell)  # pylint: disable=W0122
    try:
        return eval("metadata")  # pylint: disable=W0123
    except NameError as e:
        raise MissingMetadataException(f"Metadata not provided for feature {feature_path}") from e


def extract_raw_metadata_from_cells(cells: List[str], feature_path: str) -> RawMetadataType:
    for current_cell in cells[:]:
        if METADATA_HEADER in current_cell:
            metadata_string = remove_magic_dividers(current_cell)

            cells.remove(current_cell)

            return get_metadata_dict(metadata_string, feature_path)

    raise MissingMetadataException(f"Metadata not provided for feature {feature_path}")


def create_metadata_dataframe(metadata: Dict[str, Any]) -> DataFrame:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    return spark.createDataFrame(data=metadata, schema=get_metadata_schema())

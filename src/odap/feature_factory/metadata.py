from typing import Any, Dict, List, Optional
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from odap.common.config import TIMESTAMP_COLUMN
from odap.common.databricks import get_widget_value
from odap.common.tables import get_existing_table
from odap.common.utils import get_notebook_name, get_relative_path
from odap.feature_factory.config import get_features_table, get_metadata_table
from odap.feature_factory.exceptions import NotebookException
from odap.feature_factory.templates import resolve_metadata_template
from odap.feature_factory.type_checker import is_fillna_valid
from odap.feature_factory.metadata_schema import (
    BACKEND,
    DTYPE,
    FEATURE,
    FILLNA_VALUE,
    FILLNA_VALUE_TYPE,
    FREQUENCY,
    LAST_COMPUTE_DATE,
    LOCATION,
    NOTEBOOK_NAME,
    NOTEBOOK_ABSOLUTE_PATH,
    NOTEBOOK_RELATIVE_PATH,
    OWNER,
    START_DATE,
    VARIABLE_TYPE,
    FILLNA_WITH,
    FeatureMetadataType,
    FeaturesMetadataType,
    RawMetadataType,
    get_feature_dtype,
    get_feature_field,
    get_metadata_schema,
    get_variable_type,
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


def set_metadata_that_need_template_resolved(
    feature_df: DataFrame, parsed_features: FeaturesMetadataType, feature_path: str
):
    for parsed_feature in parsed_features:
        feature_field = get_feature_field(feature_df, parsed_feature[FEATURE], feature_path)

        parsed_feature[DTYPE] = get_feature_dtype(feature_field)
        parsed_feature[VARIABLE_TYPE] = get_variable_type(parsed_feature[DTYPE])

        resolve_fillna_with(parsed_feature)


def get_features_from_raw_metadata(raw_metadata: RawMetadataType, feature_path: str) -> FeaturesMetadataType:
    raw_features = raw_metadata.pop("features", None)

    if not raw_features:
        NotebookException("No features provided in metadata.", feature_path)

    for feature_name, value_dict in raw_features.items():
        value_dict[FEATURE] = feature_name

    return list(raw_features.values())


def check_metadata(metadata: FeatureMetadataType, feature_path: str):
    for field in metadata:
        if field not in get_metadata_schema().fieldNames():
            raise NotebookException(f"{field} is not a supported metadata field.", feature_path)

    return metadata


def check_all_metadata(features_metadata: FeaturesMetadataType, feature_path: str):
    for metadata in features_metadata:
        check_metadata(metadata, feature_path)


def get_global_metadata(raw_metadata: RawMetadataType, feature_path: str) -> FeatureMetadataType:
    check_metadata(raw_metadata, feature_path)

    set_notebook_paths(feature_path, global_metadata_dict=raw_metadata)

    return raw_metadata


def get_feature_dates(existing_metadata_df: Optional[DataFrame], feature_name: str) -> Dict[str, datetime]:
    timestamp = datetime.fromisoformat(get_widget_value(TIMESTAMP_COLUMN))

    start_date = timestamp
    last_comput_date = timestamp

    if existing_metadata_df:
        existing_dates = (
            existing_metadata_df.select(START_DATE, LAST_COMPUTE_DATE).filter(col(FEATURE) == feature_name).first()
        )

        start_date = min(start_date, existing_dates[START_DATE])
        last_comput_date = max(last_comput_date, existing_dates[LAST_COMPUTE_DATE])

    return {LAST_COMPUTE_DATE: last_comput_date, START_DATE: start_date}


def set_fs_compatible_metadata(features_metadata: FeaturesMetadataType, config: Dict[str, Any]):
    existing_metadata_df = get_existing_table(get_metadata_table(config))

    for metadata in features_metadata:
        metadata.update(get_feature_dates(existing_metadata_df, metadata[FEATURE]))
        metadata.update(
            {
                OWNER: "unknown",
                FREQUENCY: "daily",
                LOCATION: get_features_table(config),
                BACKEND: "delta_table",
            }
        )


def resolve_fillna_with(feature_metadata: FeatureMetadataType):
    if FILLNA_WITH in feature_metadata:
        fillna_with = feature_metadata.pop(FILLNA_WITH)

        feature_metadata[FILLNA_VALUE] = fillna_with
        feature_metadata[FILLNA_VALUE_TYPE] = type(fillna_with).__name__

        is_fillna_valid(feature_metadata[DTYPE], fillna_with, feature_metadata[FEATURE])
    else:
        feature_metadata[FILLNA_VALUE] = "None"
        feature_metadata[FILLNA_VALUE_TYPE] = "NoneType"


def resolve_metadata(raw_metadata: RawMetadataType, feature_path: str, feature_df: DataFrame) -> FeaturesMetadataType:
    features_metadata = []

    features = get_features_from_raw_metadata(raw_metadata, feature_path)
    global_metadata = get_global_metadata(raw_metadata, feature_path)

    for feature_metadata in features:
        feature_metadata.update(global_metadata)

        features_metadata.extend(resolve_metadata_template(feature_df, feature_metadata))

    set_metadata_that_need_template_resolved(feature_df, features_metadata, feature_path)

    check_all_metadata(features_metadata, feature_path)

    return features_metadata


def get_metadata_dict(cell: str, feature_path: str):
    cell = cell[cell.find(METADATA_HEADER) :]

    exec(cell)  # pylint: disable=W0122
    try:
        return eval("metadata")  # pylint: disable=W0123
    except NameError as e:
        raise NotebookException("Metadata not provided.", feature_path) from e


def extract_raw_metadata_from_cells(cells: List[str], feature_path: str) -> RawMetadataType:
    for current_cell in cells[:]:
        if METADATA_HEADER in current_cell:
            metadata_string = remove_magic_dividers(current_cell)

            cells.remove(current_cell)

            return get_metadata_dict(metadata_string, feature_path)

    raise NotebookException("Metadata not provided.", feature_path)

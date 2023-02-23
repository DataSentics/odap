from typing import Any, Dict, List, Optional
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from odap.feature_factory import const
from odap.common.notebook import eval_cell_with_header
from odap.common.tables import get_existing_table
from odap.common.utils import get_notebook_name, get_relative_path
from odap.common.exceptions import NotebookException
from odap.feature_factory.config import get_metadata_table
from odap.feature_factory.templates import resolve_metadata_templates
from odap.feature_factory.type_checker import check_fillna_valid
from odap.feature_factory.metadata_schema import (
    FeatureMetadataType,
    FeaturesMetadataType,
    RawMetadataType,
    get_feature_dtype,
    get_feature_field,
    get_metadata_schema,
    get_variable_type,
)


def set_notebook_paths(feature_path: str, global_metadata_dict: FeatureMetadataType):
    global_metadata_dict[const.NOTEBOOK_NAME] = get_notebook_name(feature_path)
    global_metadata_dict[const.NOTEBOOK_ABSOLUTE_PATH] = feature_path
    global_metadata_dict[const.NOTEBOOK_RELATIVE_PATH] = get_relative_path(feature_path)


def get_features_from_raw_metadata(raw_metadata: RawMetadataType, feature_path: str) -> FeaturesMetadataType:
    raw_features = raw_metadata.pop("features", None)

    if not raw_features:
        NotebookException("No features provided in metadata.", path=feature_path)

    for feature_name, value_dict in raw_features.items():
        value_dict[const.FEATURE] = feature_name.lower()

    return list(raw_features.values())


def check_metadata(metadata: FeatureMetadataType, feature_path: str):
    for field in metadata:
        if field not in get_metadata_schema().fieldNames():
            raise NotebookException(f"'{field}' is not a supported metadata field.", path=feature_path)

    if "table" not in metadata:
        raise Exception(f"Notebook at '{feature_path}' does not define 'table' in metadata.")

    return metadata


def get_global_metadata(raw_metadata: RawMetadataType, feature_path: str) -> FeatureMetadataType:
    check_metadata(raw_metadata, feature_path)

    set_notebook_paths(feature_path, global_metadata_dict=raw_metadata)

    return raw_metadata


def get_feature_dates(
    existing_metadata_df: Optional[DataFrame], feature_name: str, start_date: datetime, last_compute_date: datetime
) -> Dict[str, datetime]:
    if existing_metadata_df:
        existing_dates = (
            existing_metadata_df.select(const.START_DATE, const.LAST_COMPUTE_DATE)
            .filter(col(const.FEATURE) == feature_name)
            .first()
        )

        if getattr(existing_dates, const.START_DATE, None):
            start_date = min(start_date, existing_dates[const.START_DATE])

        if getattr(existing_dates, const.LAST_COMPUTE_DATE, None):
            last_compute_date = max(last_compute_date, existing_dates[const.LAST_COMPUTE_DATE])

    return {const.LAST_COMPUTE_DATE: last_compute_date, const.START_DATE: start_date}


def set_fs_compatible_metadata(features_metadata: FeaturesMetadataType, config: Dict[str, Any]):
    existing_metadata_df = get_existing_table(get_metadata_table(config))

    start_date = datetime.today()
    last_compute_date = datetime.today()

    for metadata in features_metadata:
        metadata.update(get_feature_dates(existing_metadata_df, metadata[const.FEATURE], start_date, last_compute_date))
        metadata.update(
            {
                const.OWNER: "unknown",
                const.FREQUENCY: "daily",
                const.BACKEND: "delta_table",
            }
        )


def resolve_fillna_with(feature_metadata: FeatureMetadataType):
    fillna_with = feature_metadata.pop(const.FILLNA_WITH, None)

    feature_metadata[const.FILLNA_VALUE] = fillna_with
    feature_metadata[const.FILLNA_VALUE_TYPE] = type(fillna_with).__name__

    check_fillna_valid(feature_metadata[const.DTYPE], fillna_with, feature_metadata[const.FEATURE])


def add_additional_metadata(metadata: FeatureMetadataType, feature_df: DataFrame, feature_path: str):
    feature_field = get_feature_field(feature_df, metadata[const.FEATURE], feature_path)

    metadata[const.DTYPE] = get_feature_dtype(feature_field)
    metadata[const.VARIABLE_TYPE] = get_variable_type(metadata[const.DTYPE])

    resolve_fillna_with(metadata)


def resolve_global_metadata(feature_metadata: FeatureMetadataType, global_metadata: FeatureMetadataType):
    for key, value in global_metadata.items():
        if key not in feature_metadata:
            feature_metadata[key] = value


def resolve_metadata(notebook_cells: List[str], feature_path: str, feature_df: DataFrame) -> FeaturesMetadataType:
    raw_metadata = extract_raw_metadata_from_cells(notebook_cells, feature_path)

    raw_features = get_features_from_raw_metadata(raw_metadata, feature_path)
    global_metadata = get_global_metadata(raw_metadata, feature_path)

    features_metadata = resolve_metadata_templates(feature_df, raw_features)

    for metadata in features_metadata:
        resolve_global_metadata(metadata, global_metadata)

        add_additional_metadata(metadata, feature_df, feature_path)

        check_metadata(metadata, feature_path)

    return features_metadata


def extract_raw_metadata_from_cells(cells: List[str], feature_path: str) -> RawMetadataType:
    raw_metadata = eval_cell_with_header(cells, feature_path, const.METADATA_HEADER_REGEX, const.METADATA)

    if raw_metadata:
        return raw_metadata

    raise NotebookException("Metadata not provided.", path=feature_path)

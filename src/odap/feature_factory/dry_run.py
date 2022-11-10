from typing import List
from pyspark.sql import DataFrame

from databricks_cli.workspace.api import WorkspaceFileInfo
from odap.common.logger import logger
from odap.common.config import TIMESTAMP_COLUMN, ConfigNamespace, get_config_namespace
from odap.common.databricks import get_workspace_api, resolve_dbutils
from odap.common.widgets import (
    DISPLAY_FEATURES,
    DISPLAY_METADATA,
    DISPLAY_WIDGET,
    get_widget_value,
    FEATURE_WIDGET,
    ALL_FEATURES,
)
from odap.common.dataframes import create_dataframe
from odap.common.utils import get_notebook_name
from odap.feature_factory.config import get_entity_primary_key
from odap.feature_factory.dataframes import create_dataframes_and_metadata, join_dataframes
from odap.feature_factory.features import get_feature_notebooks_info
from odap.feature_factory.metadata import set_fs_compatible_metadata
from odap.feature_factory.metadata_schema import get_metadata_schema


def get_list_of_selected_feature_notebooks() -> List[WorkspaceFileInfo]:
    feature_notebook_name = get_widget_value(FEATURE_WIDGET)
    feature_notebooks = get_feature_notebooks_info(get_workspace_api())

    if feature_notebook_name == ALL_FEATURES:
        return feature_notebooks

    return [
        feature_notebook for feature_notebook in feature_notebooks if feature_notebook.basename == feature_notebook_name
    ]


def dry_run():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    entity_primary_key = get_entity_primary_key(config)

    dataframes, metadata = create_dataframes_and_metadata(entity_primary_key, get_list_of_selected_feature_notebooks())

    set_fs_compatible_metadata(metadata, config)

    metadata_df = create_dataframe(metadata, get_metadata_schema())

    features_df = join_dataframes(dataframes, join_columns=[entity_primary_key, TIMESTAMP_COLUMN])

    logger.info("Success. No errors found!")

    display_dataframes(features_df, metadata_df)


def display_dataframes(features_df: DataFrame, metadata_df: DataFrame):
    display_widget_value = get_widget_value(DISPLAY_WIDGET)

    if DISPLAY_FEATURES in display_widget_value:
        display_features_df(features_df)

    if DISPLAY_METADATA in display_widget_value:
        display_metadata_df(metadata_df)


def display_metadata_df(metadata_df: DataFrame):
    print("\nMetadata DataFrame:")
    metadata_df.display()  # pyre-ignore[29]


def display_features_df(final_df: DataFrame):
    print("\nFeatures DataFrame:")
    final_df.display()  # pyre-ignore[29]


def create_dry_run_widgets():
    dbutils = resolve_dbutils()

    features = [
        get_notebook_name(notebook_info.path) for notebook_info in get_feature_notebooks_info(get_workspace_api())
    ]

    dbutils.widgets.dropdown(FEATURE_WIDGET, ALL_FEATURES, features + [ALL_FEATURES])
    dbutils.widgets.multiselect(DISPLAY_WIDGET, DISPLAY_METADATA, choices=[DISPLAY_METADATA, DISPLAY_FEATURES])

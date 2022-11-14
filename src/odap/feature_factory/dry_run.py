from typing import List
from pyspark.sql import DataFrame
from databricks_cli.workspace.api import WorkspaceFileInfo

from odap.feature_factory import const

from odap.common.logger import logger
from odap.common.config import ConfigNamespace, get_config_namespace
from odap.common.databricks import get_workspace_api, resolve_dbutils
from odap.common.widgets import get_widget_value
from odap.common.utils import get_notebook_name
from odap.feature_factory.config import get_entity_primary_key
from odap.feature_factory.dataframes.dataframe_creator import create_features_df, create_metadata_df
from odap.feature_factory.dq_checks import execute_soda_checks_from_feature_notebooks
from odap.feature_factory.feature_notebook import get_feature_notebooks_info, load_feature_notebooks


def get_list_of_selected_feature_notebooks() -> List[WorkspaceFileInfo]:
    feature_notebook_name = get_widget_value(const.FEATURE_WIDGET)
    feature_notebooks = get_feature_notebooks_info(get_workspace_api())

    if feature_notebook_name == const.ALL_FEATURES:
        return feature_notebooks

    return [
        feature_notebook for feature_notebook in feature_notebooks if feature_notebook.basename == feature_notebook_name
    ]


def dry_run():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    feature_notebooks = load_feature_notebooks(config, get_list_of_selected_feature_notebooks())

    entity_primary_key = get_entity_primary_key(config)

    features_df = create_features_df(feature_notebooks, entity_primary_key)
    metadata_df = create_metadata_df(feature_notebooks)

    logger.info("Success. No errors found!")

    execute_soda_checks_from_feature_notebooks(df=features_df, feature_notebooks=feature_notebooks)

    display_dataframes(features_df, metadata_df)


def display_dataframes(features_df: DataFrame, metadata_df: DataFrame):
    display_widget_value = get_widget_value(const.DISPLAY_WIDGET)

    if const.DISPLAY_FEATURES in display_widget_value:
        display_features_df(features_df)

    if const.DISPLAY_METADATA in display_widget_value:
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

    dbutils.widgets.dropdown(const.FEATURE_WIDGET, const.ALL_FEATURES, features + [const.ALL_FEATURES])
    dbutils.widgets.multiselect(
        const.DISPLAY_WIDGET, const.DISPLAY_METADATA, choices=[const.DISPLAY_METADATA, const.DISPLAY_FEATURES]
    )

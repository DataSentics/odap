from typing import List
from pyspark.sql import DataFrame

from databricks_cli.workspace.api import WorkspaceFileInfo
from odap.common.logger import logger
from odap.common.config import ConfigNamespace, get_config_namespace, TIMESTAMP_COLUMN
from odap.common.databricks import get_widget_value, get_workspace_api, resolve_dbutils
from odap.common.dataframes import create_dataframe
from odap.common.utils import get_notebook_name
from odap.feature_factory.config import get_entity_primary_key
from odap.feature_factory.dataframes import create_dataframes_and_metadata, join_dataframes
from odap.feature_factory.features import get_feature_notebooks_info
from odap.feature_factory.metadata import set_fs_compatible_metadata
from odap.feature_factory.metadata_schema import get_metadata_schema


ALL = "<all>"
FEATURE_WIDGET = "feature"


def get_list_of_selected_feature_notebooks() -> List[WorkspaceFileInfo]:
    feature_notebook_name = get_widget_value(FEATURE_WIDGET)
    feature_notebooks = get_feature_notebooks_info(get_workspace_api())

    if feature_notebook_name == ALL:
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

    final_df = join_dataframes(dataframes, join_columns=[entity_primary_key, TIMESTAMP_COLUMN])

    logger.info("Success. No errors found!")

    display_dataframe_table(final_df)
    display_metadata_df(metadata_df)


def display_metadata_df(metadata_df: DataFrame):
    print("\nMetadata Dataframe:")
    metadata_df.display()  # pyre-ignore[29]


def display_dataframe_table(final_df: DataFrame):
    print("\nFeatures Dataframe:")
    final_df.display()  # pyre-ignore[29]


def create_notebook_widget():
    dbutils = resolve_dbutils()

    features = [
        get_notebook_name(notebook_info.path) for notebook_info in get_feature_notebooks_info(get_workspace_api())
    ]

    dbutils.widgets.dropdown(FEATURE_WIDGET, ALL, features + [ALL])

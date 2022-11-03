import os
from typing import List

from odap.common.logger import logger
from odap.common.config import ConfigNamespace, get_config_namespace, TIMESTAMP_COLUMN
from odap.common.databricks import get_widget_value, get_workspace_api, resolve_dbutils
from odap.common.dataframes import create_dataframe
from odap.common.utils import get_absolute_path, get_notebook_name
from odap.feature_factory.config import get_entity_primary_key
from odap.feature_factory.dataframes import create_dataframes_and_metadata, join_dataframes
from odap.feature_factory.features import get_features_paths
from odap.feature_factory.metadata import set_fs_compatible_metadata
from odap.feature_factory.metadata_schema import get_metadata_schema


ALL = "<all>"
FEATURE_WIDGET = "feature"


def get_list_of_selected_features() -> List[str]:
    feature_name = get_widget_value(FEATURE_WIDGET)

    if feature_name == ALL:
        return get_features_paths(get_workspace_api())

    return [os.path.join(get_absolute_path("features"), feature_name)]


def dry_run():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    entity_primary_key = get_entity_primary_key(config)

    dataframes, metadata = create_dataframes_and_metadata(
        entity_primary_key, features_paths=get_list_of_selected_features()
    )

    set_fs_compatible_metadata(metadata, config)

    metadata_df = create_dataframe(metadata, get_metadata_schema())

    if len(dataframes) > 1:
        join_dataframes(dataframes, join_columns=[entity_primary_key, TIMESTAMP_COLUMN])

    logger.info("Success. No errors found!\nMetadata Table:")

    metadata_df.display()


def create_notebook_widget():
    dbutils = resolve_dbutils()

    features = [get_notebook_name(path) for path in get_features_paths(get_workspace_api())]

    dbutils.widgets.dropdown(FEATURE_WIDGET, ALL, features + [ALL])

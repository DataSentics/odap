from odap.common.config import TIMESTAMP_COLUMN, get_config_namespace, ConfigNamespace
from odap.common.dataframes import create_dataframe
from odap.common.databricks import get_workspace_api
from odap.feature_factory.config import (
    get_entity,
    get_entity_primary_key,
    get_features_table,
    get_latest_features_table,
    get_latest_features_table_path,
    get_features_table_path,
    get_metadata_table,
    get_metadata_table_path,
)
from odap.feature_factory.dataframes import create_dataframes_and_metadata, join_dataframes, get_latest_features
from odap.feature_factory.features import get_feature_notebooks_info
from odap.feature_factory.feature_store import write_df_to_feature_store
from odap.feature_factory.metadata import set_fs_compatible_metadata
from odap.feature_factory.metadata_schema import get_metadata_schema


# pylint: disable=too-many-statements
def orchestrate():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    entity_name = get_entity(config)
    entity_primary_key = get_entity_primary_key(config)
    feature_notebooks = get_feature_notebooks_info(get_workspace_api())

    dataframes, metadata = create_dataframes_and_metadata(entity_primary_key, feature_notebooks)

    set_fs_compatible_metadata(metadata, config)

    df = join_dataframes(dataframes, join_columns=[entity_primary_key, TIMESTAMP_COLUMN])
    metadata_df = create_dataframe(metadata, get_metadata_schema())

    write_df_to_feature_store(
        df,
        table_name=get_features_table(config),
        table_path=get_features_table_path(config),
        primary_keys=[entity_primary_key, TIMESTAMP_COLUMN],
        partition_columns=[TIMESTAMP_COLUMN],
    )

    (
        metadata_df.write.mode("overwrite")
        .option("overwriteSchema", True)
        .option("path", get_metadata_table_path(config))
        .saveAsTable(get_metadata_table(config))
    )

    latest_features = get_latest_features(entity_name, config)

    (
        latest_features.write.mode("overwrite")
        .option("overwriteSchema", True)
        .option("path", get_latest_features_table_path(config))
        .saveAsTable(get_latest_features_table(config))
    )

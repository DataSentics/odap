from odap.common.config import TIMESTAMP_COLUMN, get_config_namespace, ConfigNamespace
from odap.feature_factory.config import (
    get_entity_primary_key,
    get_features_table,
    get_features_table_path,
    get_metadata_table,
    get_metadata_table_path,
)
from odap.feature_factory.dataframes import create_dataframes_and_metadata, join_dataframes
from odap.feature_factory.feature_store import write_df_to_feature_store
from odap.feature_factory.metadata import create_metadata_dataframe


def orchestrate():
    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)
    entity_primary_key = get_entity_primary_key(config)

    dataframes, metadata = create_dataframes_and_metadata()

    df = join_dataframes(dataframes, join_columns=[entity_primary_key])
    metadata_df = create_metadata_dataframe(metadata)

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
        .saveAsTable(f"{get_metadata_table(config)}")
    )

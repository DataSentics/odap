from pyspark.sql import SparkSession
from delta import DeltaTable
from odap.common.config import TIMESTAMP_COLUMN, Config
from odap.common.tables import create_table_if_not_exists
from odap.feature_factory.config import (
    get_entity,
    get_entity_primary_key,
    get_features_table,
    get_features_table_path,
    get_latest_features_table,
    get_latest_features_table_path,
    get_metadata_table,
    get_metadata_table_path,
)
from odap.feature_factory.dataframes.dataframe_creator import (
    create_metadata_df,
    create_features_df,
    get_latest_features,
)
from odap.feature_factory.feature_store import write_df_to_feature_store
from odap.feature_factory.metadata_schema import get_metadata_pk_columns, get_metadata_columns, get_metadata_schema
from odap.feature_factory.feature_notebook import FeatureNotebooks


def write_metadata_df(feature_notebooks: FeatureNotebooks, config: Config):
    create_table_if_not_exists(get_metadata_table(config), get_metadata_table_path(config), get_metadata_schema())
    metadata_df = create_metadata_df(feature_notebooks)
    delta_table = DeltaTable.forName(SparkSession.getActiveSession(), get_metadata_table(config))
    metadata_pk_columns = get_metadata_pk_columns()
    metadata_columns = get_metadata_columns()

    update_set = {col.name: f"source.{col.name}" for col in metadata_columns}
    insert_set = {**{col.name: f"source.{col.name}" for col in metadata_pk_columns}, **update_set}
    merge_condition = " AND ".join(f"target.{col.name} = source.{col.name}" for col in metadata_pk_columns)

    (
        delta_table.alias("target")
        .merge(metadata_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )


def write_features_df(feature_notebooks: FeatureNotebooks, config: Config):
    entity_primary_key = get_entity_primary_key(config)

    df = create_features_df(feature_notebooks, entity_primary_key)

    write_df_to_feature_store(
        df,
        table_name=get_features_table(config),
        table_path=get_features_table_path(config),
        primary_keys=[entity_primary_key, TIMESTAMP_COLUMN],
        partition_columns=[TIMESTAMP_COLUMN],
    )


def write_latest_features(config: Config):
    entity_name = get_entity(config)

    latest_features = get_latest_features(entity_name, config)

    (
        latest_features.write.mode("overwrite")
        .option("overwriteSchema", True)
        .option("path", get_latest_features_table_path(config))
        .saveAsTable(get_latest_features_table(config))
    )

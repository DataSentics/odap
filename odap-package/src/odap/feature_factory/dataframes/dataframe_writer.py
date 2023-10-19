from typing import Dict, Iterable

from pyspark.sql import SparkSession, functions as f, DataFrame
from delta import DeltaTable

from odap.common.config import TIMESTAMP_COLUMN
from odap.common.databricks import spark
from odap.common.tables import create_table_if_not_exists, create_schema_if_not_exists
from odap.feature_factory.config import Config
from odap.feature_factory.dataframes.dataframe_creator import (
    create_metadata_df,
    create_features_df,
    fill_nulls,
)
from odap.feature_factory.feature_store import write_df_to_feature_store, write_latest_table
from odap.feature_factory.metadata_schema import get_metadata_pk_columns, get_metadata_columns, get_metadata_schema
from odap.feature_factory.feature_notebook import FeatureNotebookList


def write_metadata_df(feature_notebooks: FeatureNotebookList, config: Config):
    create_schema_if_not_exists(config)
    create_table_if_not_exists(config.get_metadata_table(), config.get_metadata_table_path(), get_metadata_schema())
    metadata_df = create_metadata_df(feature_notebooks)
    delta_table = DeltaTable.forName(SparkSession.getActiveSession(), config.get_metadata_table())
    metadata_pk_columns = get_metadata_pk_columns()

    update_set = {col.name: f"source.{col.name}" for col in get_metadata_columns()}
    insert_set = {**{col.name: f"source.{col.name}" for col in metadata_pk_columns}, **update_set}
    merge_condition = " AND ".join(f"target.{col.name} = source.{col.name}" for col in metadata_pk_columns)

    (
        delta_table.alias("target")
        .merge(metadata_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )


def get_table_df_mapping(
    notebook_table_mapping: Dict[str, FeatureNotebookList], config: Config
) -> Dict[str, DataFrame]:
    entity_primary_key = config.get_entity_primary_key()

    return {
        table_name: create_features_df(feature_notebooks_subset, entity_primary_key)
        for table_name, feature_notebooks_subset in notebook_table_mapping.items()
    }


def write_features_df(table_df_mapping: Dict[str, DataFrame], config: Config):
    create_schema_if_not_exists(config)
    entity_primary_key = config.get_entity_primary_key()

    for table_name, df in table_df_mapping.items():
        write_df_to_feature_store(
            df,
            table_name=config.get_features_table(table_name),
            table_path=config.get_features_table_path(table_name),
            primary_keys=[entity_primary_key],
            timestamp_keys=[TIMESTAMP_COLUMN],
        )


def get_latest_dataframe(feature_tables: Iterable[str], config: Config):
    spark.sparkContext.setCheckpointDir(config.get_checkpoint_dir())

    features_dfs = [spark.table(config.get_features_table(table)) for table in feature_tables]

    features_dfs_max_date = [(df, df.select(f.max(TIMESTAMP_COLUMN)).collect()[0][0]) for df in features_dfs]

    features_dfs_max_date_filtered = [
        df.filter(f.col(TIMESTAMP_COLUMN) == max_ts).drop(TIMESTAMP_COLUMN) for df, max_ts in features_dfs_max_date
    ]

    latest_df = features_dfs_max_date_filtered[0]

    for i, df in enumerate(features_dfs_max_date_filtered[1:]):
        latest_df = latest_df.join(df, on=config.get_entity_primary_key(), how="full")
        if not i % config.get_checkpoint_interval():
            latest_df.checkpoint()
    return latest_df


def write_latest_features(feature_notebooks: FeatureNotebookList, config: Config):
    create_schema_if_not_exists(config)
    metadata_df = spark.table(config.get_metadata_table())
    feature_tables = [row.table for row in metadata_df.select("table").distinct().collect()]

    latest_df = get_latest_dataframe(feature_tables, config)
    latest_features_filled = fill_nulls(latest_df, feature_notebooks)

    latest_table_path = config.get_latest_features_table_path()
    latest_table_name = config.get_latest_features_table()

    write_latest_table(latest_features_filled, latest_table_name, latest_table_path)

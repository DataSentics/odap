from typing import Dict, Iterable

from pyspark.sql import SparkSession, functions as f
from delta import DeltaTable

from odap.common.config import TIMESTAMP_COLUMN, Config
from odap.common.databricks import spark
from odap.common.tables import create_table_if_not_exists
from odap.feature_factory.config import (
    get_entity_primary_key,
    get_features_table,
    get_features_table_path,
    get_latest_features_table,
    get_latest_features_table_path,
    get_metadata_table,
    get_metadata_table_path,
    get_checkpoint_dir,
    get_checkpoint_interval,
)
from odap.feature_factory.dataframes.dataframe_creator import (
    create_metadata_df,
    create_features_df,
    fill_nulls,
)
from odap.feature_factory.feature_store import write_df_to_feature_store, write_latest_table
from odap.feature_factory.metadata_schema import get_metadata_pk_columns, get_metadata_columns, get_metadata_schema
from odap.feature_factory.feature_notebook import FeatureNotebookList


def write_metadata_df(feature_notebooks: FeatureNotebookList, config: Config):
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


def write_features_df(notebook_table_mapping: Dict[str, FeatureNotebookList], config: Config):
    entity_primary_key = get_entity_primary_key(config)

    for table_name, feature_notebooks_subset in notebook_table_mapping.items():
        df = create_features_df(feature_notebooks_subset, entity_primary_key)

        write_df_to_feature_store(
            df,
            table_name=get_features_table(table_name, config),
            table_path=get_features_table_path(table_name, config),
            primary_keys=[entity_primary_key],
            timestamp_keys=[TIMESTAMP_COLUMN],
        )


def get_latest_dataframe(feature_tables: Iterable[str], config: Config):
    spark.sparkContext.setCheckpointDir(get_checkpoint_dir(config))

    features_dfs = [spark.table(get_features_table(table, config)) for table in feature_tables]

    features_dfs_max_date = [(df, df.select(f.max(TIMESTAMP_COLUMN)).collect()[0][0]) for df in features_dfs]

    features_dfs_max_date_filtered = [
        df.filter(f.col(TIMESTAMP_COLUMN) == max_ts).drop(TIMESTAMP_COLUMN) for df, max_ts in features_dfs_max_date
    ]

    latest_df = features_dfs_max_date_filtered[0]

    for i, df in enumerate(features_dfs_max_date_filtered[1:]):
        latest_df = latest_df.join(df, on=get_entity_primary_key(config), how="full")
        if not i % get_checkpoint_interval(config):
            latest_df.checkpoint()
    return latest_df


def write_latest_features(feature_notebooks: FeatureNotebookList, config: Config):
    metadata_df = spark.table(get_metadata_table(config))
    feature_tables = [row.table for row in metadata_df.select("table").distinct().collect()]

    latest_df = get_latest_dataframe(feature_tables, config)
    latest_features_filled = fill_nulls(latest_df, feature_notebooks)

    latest_table_path = get_latest_features_table_path(config)
    latest_table_name = get_latest_features_table(config)

    write_latest_table(latest_features_filled, latest_table_name, latest_table_path)

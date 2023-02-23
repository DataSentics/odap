from typing import Dict

from pyspark.sql import SparkSession
from databricks.feature_store import FeatureStoreClient
from delta import DeltaTable

from odap.common.config import TIMESTAMP_COLUMN, Config
from odap.common.tables import create_table_if_not_exists
from odap.feature_factory.config import (
    get_entity_primary_key,
    get_features_table,
    get_features_table_path,
    get_latest_features_table,
    get_latest_features_table_path,
    get_metadata_table,
    get_metadata_table_path,
    get_entity,
)
from odap.feature_factory.dataframes.dataframe_creator import (
    create_metadata_df,
    create_features_df,
    fill_nulls,
)
from odap.feature_factory.feature_store import write_df_to_feature_store, write_latest_table, generate_feature_lookups
from odap.feature_factory.ids import get_latest_ids
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


def write_latest_features(feature_notebooks: FeatureNotebookList, config: Config):
    fs = FeatureStoreClient()
    entity_name = get_entity(config)

    ids_df = get_latest_ids(config)

    latest_df = (
        fs.create_training_set(ids_df, generate_feature_lookups(entity_name), label=None)
        .load_df()
        .drop(TIMESTAMP_COLUMN)
    )
    latest_features_filled = fill_nulls(latest_df, feature_notebooks)

    latest_table_path = get_latest_features_table_path(config)
    latest_table_name = get_latest_features_table(config)

    write_latest_table(latest_features_filled, latest_table_name, latest_table_path)

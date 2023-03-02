from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame, functions as f
from databricks.feature_store import FeatureStoreClient, FeatureLookup

from odap.common.config import ConfigNamespace, get_config_namespace
from odap.common.logger import logger
from odap.common.tables import hive_table_exists
from odap.common.dataframes import get_values_missing_from_df_column
from odap.feature_factory.config import (
    get_features_table,
    get_entity_primary_key_by_name,
    get_metadata_table_for_entity,
)


def create_feature_store_table(
    fs: FeatureStoreClient,
    df: DataFrame,
    table_name: str,
    table_path: Optional[str],
    primary_keys: List[str],
    timestamp_keys: List[str],
) -> None:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    catalog, database, _ = table_name.split(".")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")

    if hive_table_exists(table_name):
        return

    kwargs = {
        "name": table_name,
        "schema": df.schema,
        "primary_keys": primary_keys,
        "timestamp_keys": timestamp_keys,
    }
    if table_path:
        logger.info(f"Path in config, saving '{table_name}' to '{table_path}'")
        kwargs["path"] = table_path

    fs.create_table(**kwargs)  # pyre-ignore[6]


def write_df_to_feature_store(
    df: DataFrame,
    table_name: str,
    table_path: Optional[str],
    primary_keys: List[str],
    timestamp_keys: List[str],
) -> None:
    fs = FeatureStoreClient()

    create_feature_store_table(fs, df, table_name, table_path, primary_keys, timestamp_keys)

    logger.info(f"Writing data to table: {table_name}...")
    fs.write_table(table_name, df=df, mode="merge")
    logger.info("Write successful.")


# pylint: disable=too-many-statements
def write_latest_table(latest_features_df: DataFrame, latest_table_name: str, latest_table_path: Optional[str]):
    logger.info(f"Writing latest data to table: '{latest_table_name}'")

    options = {"mergeSchema": "true"}

    if latest_table_path:
        logger.info(f"Path in config, saving '{latest_table_name}' to '{latest_table_path}'")
        options["path"] = latest_table_path

    (latest_features_df.write.mode("overwrite").options(**options).saveAsTable(latest_table_name))
    logger.info("Write successful.")


def check_df_column_contains_values(df: DataFrame, column: str, values: List[str]):
    missing_values = get_values_missing_from_df_column(df, column, values)

    if missing_values:
        if len(missing_values) == 1:
            raise Exception(f"Value `{missing_values[0]}` is not present in column `{column}`")

        raise Exception(f"Values {missing_values} are not present in column `{column}`")


# pylint: disable=too-many-statements
def generate_feature_lookups(
    entity_name: str, features: Optional[List[str]] = None, categories: Optional[List[str]] = None
) -> List[FeatureLookup]:
    features = features if features is not None else []
    categories = categories if categories is not None else []
    lookup_all_features = not features and not categories

    config = get_config_namespace(ConfigNamespace.FEATURE_FACTORY)

    entity_primary_key = get_entity_primary_key_by_name(entity_name, config)
    metadata_table = get_metadata_table_for_entity(entity_name, config)

    metadata = SparkSession.getActiveSession().read.table(metadata_table)

    check_df_column_contains_values(df=metadata, column="category", values=categories)
    check_df_column_contains_values(df=metadata, column="feature", values=features)

    if not lookup_all_features:
        metadata = metadata.filter(metadata.feature.isin(features) | metadata.category.isin(categories))

    table_to_features_agg = metadata.groupby("table").agg(f.collect_set("feature").alias("features"))

    return [
        FeatureLookup(
            table_name=get_features_table(row.table, config),
            feature_names=row.features,
            lookup_key=entity_primary_key,
            timestamp_lookup_key="timestamp",
        )
        for row in table_to_features_agg.collect()
    ]

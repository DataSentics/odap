from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from databricks.feature_store import FeatureStoreClient

from odap.common.logger import logger
from odap.common.tables import hive_table_exists


def create_feature_store_table(
    fs: FeatureStoreClient,
    df: DataFrame,
    table_name: str,
    table_path: Optional[str],
    primary_keys: List[str],
    timestamp_keys: List[str],
) -> None:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.split('.')[1]}")

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


def write_latest_table(latest_features_df: DataFrame, latest_table_name: str, latest_table_path: Optional[str]):
    logger.info(f"Writing latest data to table: '{latest_table_name}'")

    options = {"mergeSchema": "true"}

    if latest_table_path:
        logger.info(f"Path in config, saving '{latest_table_name}' to '{latest_table_path}'")
        options["path"] = latest_table_path

    (latest_features_df.write.mode("overwrite").options(**options).saveAsTable(latest_table_name))
    logger.info("Write successful.")

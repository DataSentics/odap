from typing import List, Union
from pyspark.sql import SparkSession, DataFrame
from databricks.feature_store import FeatureStoreClient


def hive_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    db_name = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    databases = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]

    if db_name not in databases:
        return False

    return spark.sql(f'SHOW TABLES IN {db_name} LIKE "{table_name}"').collect() != []


def create_feature_store_table(
    fs: FeatureStoreClient,
    df: DataFrame,
    table_name: str,
    table_path: str,
    primary_keys: List[str],
    partition_columns: Union[List[str], None]
) -> None:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.split('.')[0]}")

    if hive_table_exists(spark, table_name):
        return

    partition_columns = partition_columns or []

    fs.create_table(
        name=table_name,
        path=table_path,
        schema=df.schema,
        primary_keys=primary_keys,
        partition_columns=partition_columns,
    )


def write_df_to_feature_store(
    df: DataFrame,
    table_name: str,
    table_path: str,
    primary_keys: List[str],
    partition_columns: Union[List[str], None] = None
    ) -> None:
    fs = FeatureStoreClient()

    create_feature_store_table(fs, df, table_name, table_path, primary_keys, partition_columns)

    fs.write_table(table_name, df=df, mode="overwrite")
from pyspark.sql import SparkSession, DataFrame
from databricks.feature_store import FeatureStoreClient
from odap.common.config import TIMESTAMP_COLUMN


def hive_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    db_name = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    databases = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]

    if db_name not in databases:
        return False

    return spark.sql(f'SHOW TABLES IN {db_name} LIKE "{table_name}"').collect() != []


def create_feature_store_table(
    fs: FeatureStoreClient, df: DataFrame, table_name: str, table_path: str, primary_key: str
) -> None:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {table_name.split('.')[0]}")

    if hive_table_exists(spark, table_name):
        return

    fs.create_table(
        name=table_name,
        path=table_path,
        schema=df.schema,
        primary_keys=[primary_key, TIMESTAMP_COLUMN],
        partition_columns=TIMESTAMP_COLUMN,
    )


def write_df_to_feature_store(df: DataFrame, table_name: str, table_path: str, primary_key: str) -> None:
    fs = FeatureStoreClient()

    create_feature_store_table(fs, df, table_name, table_path, primary_key)

    fs.write_table(table_name, df=df, mode="overwrite")

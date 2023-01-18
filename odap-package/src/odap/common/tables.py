from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from delta import DeltaTable
from databricks.feature_store import FeatureStoreClient

from odap.common.logger import logger


def hive_table_exists(full_table_name: str) -> bool:
    spark = SparkSession.getActiveSession()

    db_name = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    databases = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]

    if db_name not in databases:
        return False

    # pylint: disable=use-implicit-booleaness-not-comparison
    return spark.sql(f'SHOW TABLES IN {db_name} LIKE "{table_name}"').collect() != []


def feature_store_table_exists(full_table_name: str) -> bool:
    feature_store_client = FeatureStoreClient()

    try:
        feature_store_client.get_table(full_table_name)
        return True

    except Exception:  # noqa pylint: disable=broad-except
        return False


def get_existing_table(table_name: str) -> Optional[DataFrame]:
    spark = SparkSession.getActiveSession()

    if hive_table_exists(table_name):
        return spark.read.table(table_name)

    return None


def create_table_if_not_exists(table_name: str, path: Optional[str], schema: StructType):
    spark = SparkSession.getActiveSession()

    table = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(schema)

    if path:
        logger.info(f"Path in config, saving '{table_name}' to '{path}'")
        table = table.location(path)

    table.execute()

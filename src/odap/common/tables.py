from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from delta import DeltaTable


def hive_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    db_name = full_table_name.split(".")[0]
    table_name = full_table_name.split(".")[1]
    databases = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]

    if db_name not in databases:
        return False

    return spark.sql(f'SHOW TABLES IN {db_name} LIKE "{table_name}"').collect() != []


def get_existing_table(table_name: str) -> Optional[DataFrame]:
    spark = SparkSession.getActiveSession()  # pylint: disable=W0641

    if hive_table_exists(spark, table_name):
        return spark.read.table(table_name)

    return None


def create_table_if_not_exists(table_name: str, path: str, schema: StructType):
    spark = SparkSession.getActiveSession()

    DeltaTable.createIfNotExists(spark).tableName(table_name).location(path).addColumns(schema).execute()

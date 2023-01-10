from pyspark.sql import SparkSession
from odap.common.config import get_config_namespace, ConfigNamespace
from odap.use_case.schemas import get_use_case_schema
from odap.use_case.usecases import generate_use_cases


def write_use_case_table():
    spark = SparkSession.getActiveSession()
    table = get_config_namespace(ConfigNamespace.USECASE_FACTORY)["table"]
    data = spark.createDataFrame(data=generate_use_cases(), schema=get_use_case_schema())

    data.write.mode("overwrite").saveAsTable(table)

from pyspark.sql import SparkSession

from odap.common.logger import logger
from odap.use_case.config import get_use_case_table
from odap.use_case.schemas import get_use_case_schema
from odap.use_case.usecases import generate_use_cases


def write_use_case_table():
    spark = SparkSession.getActiveSession()
    table = get_use_case_table()
    data = spark.createDataFrame(data=generate_use_cases(), schema=get_use_case_schema())

    logger.info(f"Saving use cases to '{table}'")
    data.write.mode("overwrite").saveAsTable(table)

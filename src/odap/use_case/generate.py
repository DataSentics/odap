from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from odap.common.config import get_config_namespace, ConfigNamespace
from odap.use_case.schemas import get_use_case_schema
from odap.use_case.usecases import generate_usecases


def table_name():
    spark = SparkSession.getActiveSession()
    path = get_config_namespace(ConfigNamespace.USECASE_FACTORY)["path"]
    data = spark.createDataFrame(
        data=generate_usecases(), schema=get_use_case_schema())

    data.write.mode("overwrite").saveAsTable(path)

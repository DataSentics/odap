from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from odap.common.config import get_config_namespace, ConfigNamespace
from odap.use_case.schemas import get_use_case_schema
from odap.use_case.usecases import generate_usecases


def generate_table():
    spark = SparkSession.getActiveSession()
    path = get_config_namespace(ConfigNamespace.USECASE_FACTORY)["path"]
    data = spark.createDataFrame(data=generate_usecases(), schema=get_use_case_schema())

    try:
        spark.sql(f"SELECT * FROM {path}")
        data.write.saveAsTable(path)
    except AnalysisException:
        df_table = spark.sql(f"select * from {path}")
        data.join(
            df_table,
            [
                "name",
                "description",
                "owner",
                "kpi",
                "status",
                "destinations",
                "exports",
                "segments",
                "model",
                "attributes",
                "sdms",
                "data_sources",
            ],
            "left_outer",
        ).write.mode("overwrite").saveAsTable(path)

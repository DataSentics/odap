from pyspark.sql import DataFrame, SparkSession
from odap.common.config import get_config_namespace, ConfigNamespace
from odap.use_case.schemas import get_use_case_schema
from odap.use_case.usecases import generate_usecases


def generate_table():
    spark = SparkSession.getActiveSession()
    PATH = get_config_namespace(ConfigNamespace.USECASE_FACTORY)["path"]
    DATA = spark.createDataFrame(data=generate_usecases(), schema=get_use_case_schema())
    if not spark.catalog.tableExists(PATH):
        DATA.write.saveAsTable(PATH)
    else:
        df_table = spark.sql(f"select * from {PATH}")
        DATA.join(
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
        ).write.mode("overwrite").saveAsTable(PATH)

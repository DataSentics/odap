from pyspark.sql.types import StructField, StructType, StringType, ArrayType, MapType


def get_use_case_schema():
    return StructType(
        [
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("kpi", ArrayType(StringType()), True),
            StructField("status", StringType(), True),
            StructField("destinations", ArrayType(StringType()), True),
            StructField("exports", ArrayType(StringType()), True),
            StructField("segments", ArrayType(StringType()), True),
            StructField("model", StringType(), True),
            StructField("attributes", MapType(StringType(), ArrayType(StringType())), True),
            StructField("sdms", ArrayType(StringType()), True),
            StructField("data_sources", ArrayType(StringType()), True),
        ]
    )

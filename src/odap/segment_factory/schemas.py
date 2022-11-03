import pyspark.sql.types as t


def get_export_schema():
    return t.StructType(
        [
            t.StructField("export_id", t.StringType(), False),
            t.StructField("timestamp", t.TimestampType(), False),
            t.StructField("segment", t.StringType(), False),
            t.StructField("segment_config", t.StringType(), False),
            t.StructField("export", t.StringType(), False),
            t.StructField("export_config", t.StringType(), False),
            t.StructField("export_type", t.StringType(), False),
            t.StructField("table", t.StringType(), False),
            t.StructField("branch", t.StringType(), False),
            t.StructField("head_commit_id", t.StringType(), False),
        ]
    )


def get_segment_common_fields_schema():
    return t.StructType(
        [
            t.StructField("export_id", t.StringType(), False),
            t.StructField("timestamp", t.TimestampType(), False),
            t.StructField("export", t.StringType(), False),
        ]
    )

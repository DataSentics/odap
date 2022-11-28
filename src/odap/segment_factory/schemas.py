import pyspark.sql.types as t

SEGMENT = "segment"
EXPORT_ID = "export_id"
TIMESTAMP = "timestamp"
USE_CASE = "use_case"
EXPORT = "export"
DESTINATION = "destination"
SEGMENTS = "segments"
DESTINATION_CONFIG = "destination_config"
SEGMENT_CONFIG = "segment_config"
DESTINATION_TYPE = "destination_type"
BRANCH = "branch"
HEAD_COMMIT_ID = "head_commit_id"


def get_export_schema():
    return t.StructType(
        [
            t.StructField(EXPORT_ID, t.StringType(), False),
            t.StructField(TIMESTAMP, t.TimestampType(), False),
            t.StructField(USE_CASE, t.StringType(), False),
            t.StructField(EXPORT, t.StringType(), False),
            t.StructField(DESTINATION, t.StringType(), False),
            t.StructField(SEGMENTS, t.ArrayType(t.StringType()), False),
            t.StructField(DESTINATION_CONFIG, t.StringType(), False),
            t.StructField(SEGMENT_CONFIG, t.StringType(), False),
            t.StructField(DESTINATION_TYPE, t.StringType(), False),
            t.StructField(BRANCH, t.StringType(), False),
            t.StructField(HEAD_COMMIT_ID, t.StringType(), False),
        ]
    )


def get_segment_common_fields_schema():
    return t.StructType(
        [
            t.StructField(EXPORT_ID, t.StringType(), False),
        ]
    )

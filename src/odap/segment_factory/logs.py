import json
import uuid
from typing import Dict
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from odap.common.logger import logger
from odap.common.utils import get_repository_info
from odap.common.databricks import get_repos_api, get_workspace_api
from odap.segment_factory.schemas import get_export_schema
from odap.segment_factory.segments import write_segment
from odap.segment_factory.config import get_export, get_log_table, get_log_table_path, get_segment

# pylint: disable=too-many-statements
def write_export_log(segment_df: DataFrame, segment_name: str, export_name: str, segment_factory_config: Dict):
    spark = SparkSession.getActiveSession()

    segment_config = get_segment(segment_name, segment_factory_config)
    export_config = get_export(export_name, segment_factory_config)

    repository = get_repository_info(workspace_api=get_workspace_api(), repos_api=get_repos_api())

    export_id = str(uuid.uuid4())
    timestamp = datetime.now()

    write_segment(segment_df, segment_name, export_id, export_name, timestamp, segment_factory_config)

    logger.info(f"Writing export log '{export_id}'")
    (
        spark.createDataFrame(
            [
                [
                    export_id,
                    timestamp,
                    segment_name,
                    export_name,
                    json.dumps(segment_config),
                    json.dumps(export_config),
                    export_config["type"],
                    repository["branch"],
                    repository["head_commit_id"],
                ]
            ],
            get_export_schema(),
        )
        .write.mode("append")
        .option("path", get_log_table_path(segment_factory_config))
        .saveAsTable(get_log_table(segment_factory_config))
    )

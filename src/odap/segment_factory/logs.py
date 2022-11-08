import json
import uuid
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from odap.common.logger import logger
from odap.common.utils import get_repository_info
from odap.common.databricks import get_repos_api, get_workspace_api
from odap.segment_factory.schemas import get_export_schema
from odap.segment_factory.segments import write_segment
from odap.segment_factory.config import get_destination, get_export, get_log_table, get_log_table_path


def write_export_log(segment_df: DataFrame, export_name: str, segment_factory_config):
    spark = SparkSession.getActiveSession()

    export_config = get_export(export_name, segment_factory_config)
    destination_config = get_destination(export_config["destination"], segment_factory_config)

    repository = get_repository_info(workspace_api=get_workspace_api(), repos_api=get_repos_api())

    export_id = str(uuid.uuid4())
    timestamp = datetime.now()

    write_segment(segment_df, export_id, segment_factory_config)

    logger.info(f"Writing export log '{export_id}'")
    (
        spark.createDataFrame(
            [
                [
                    export_id,
                    timestamp,
                    export_name,
                    export_config["destination"],
                    list(export_config["segments"].keys()),
                    json.dumps(export_config),
                    json.dumps(destination_config),
                    destination_config["type"],
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

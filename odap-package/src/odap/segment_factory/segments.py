import re
from functools import reduce
from typing import Any, Dict
from pyspark.sql import DataFrame, SparkSession, types as t, functions as f
from odap.common.logger import logger
from odap.common.databricks import get_workspace_api
from odap.common.dataframes import create_dataframe, create_dataframe_from_notebook_cells
from odap.feature_factory.config import get_features_table
from odap.segment_factory.config import get_segment_table, get_segment_table_path, USE_CASES_FOLDER, SEGMENTS_FOLDER
from odap.common.utils import get_absolute_api_path
from odap.common.notebook import get_notebook_cells, get_notebook_info
from odap.segment_factory.exceptions import SegmentNotFoundException
from odap.segment_factory.schemas import SEGMENT, get_segment_common_fields_schema


def write_segment(
    df: DataFrame,
    export_id: str,
    segment_factory_config: Dict,
):

    extended_segment_df = create_dataframe([[export_id]], get_segment_common_fields_schema()).join(df, how="full")

    table = get_segment_table(segment_factory_config)
    logger.info(f"Writing segments to hive table {table}")

    options = {"mergeSchema": "true"}

    if path := get_segment_table_path(segment_factory_config):
        logger.info(f"Path in config, saving '{table}' to '{path}'")
        options["path"] = path

    (extended_segment_df.write.format("delta").mode("append").options(**options).saveAsTable(table))


def create_segment_df(segment_name: str, use_case_name: str) -> DataFrame:
    workspace_api = get_workspace_api()

    segment_path = get_absolute_api_path(USE_CASES_FOLDER, use_case_name, SEGMENTS_FOLDER, segment_name)
    notebook_info = get_notebook_info(segment_path, workspace_api)

    notebook_cells = get_notebook_cells(notebook_info, workspace_api)
    last_cell = notebook_cells[-1]
    pattern = re.compile(r"where.*", re.MULTILINE | re.DOTALL | re.IGNORECASE)
    where_clause = pattern.search(last_cell).group(0)

    # TODO get feature table
    # TODO get entity id
    # TODO run select and where on feature table + run_date filter
    feature_table = "dev.odap_features_customer.simple_features"
    entity_id = "customer_id"
    sql_cmd = f"""select {entity_id} from {feature_table} {where_clause}"""
    return SparkSession.getActiveSession().sql(sql_cmd)


def create_segments_union_df(segments_config: Dict[str, Any], use_case_name: str) -> DataFrame:
    spark = SparkSession.getActiveSession()
    empty_df = spark.createDataFrame([], t.StructType([]))

    def union_segments(prev_df: DataFrame, segment_name: str):
        segment_df = create_segment_df(segment_name, use_case_name)
        segment_df = segment_df.withColumn(SEGMENT, f.lit(segment_name)).select("segment", *segment_df.columns)
        return prev_df.unionByName(segment_df, allowMissingColumns=True)

    return reduce(union_segments, segments_config.keys(), empty_df)

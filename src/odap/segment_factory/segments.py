from functools import reduce
from typing import Any, Dict
from pyspark.sql import DataFrame, SparkSession, types as t, functions as f
from odap.common.logger import logger
from odap.common.databricks import get_workspace_api
from odap.common.dataframes import create_dataframe, create_dataframe_from_notebook_cells
from odap.segment_factory.config import get_segment_table, get_segment_table_path
from odap.common.utils import get_absolute_path, get_notebook_cells, get_notebook_language
from odap.segment_factory.exceptions import SegmentNotFoundException
from odap.segment_factory.schemas import SEGMENT, get_segment_common_fields_schema


def write_segment(
    df: DataFrame,
    export_id: str,
    segment_factory_config: Dict,
):

    extended_segment_df = create_dataframe([[export_id]], get_segment_common_fields_schema()).join(df, how="full")

    table = get_segment_table(segment_factory_config)
    logger.info(f"Writing segment to table: '{table}'")
    (
        extended_segment_df.write.format("delta")
        .mode("append")
        .option("path", get_segment_table_path(segment_factory_config))
        .option("mergeSchema", "True")
        .saveAsTable(table)
    )


def create_segment_df(segment_name: str) -> DataFrame:
    workspace_api = get_workspace_api()

    segment_path = get_absolute_path("segments", segment_name)

    notebook_cells = get_notebook_cells(segment_path, workspace_api)
    notebook_language = get_notebook_language(segment_path, workspace_api)
    segment_df = create_dataframe_from_notebook_cells(segment_path, notebook_language, notebook_cells)

    if not segment_df:
        raise SegmentNotFoundException(f"Segment '{segment_name}' could not be loaded")
    return segment_df


def union_segments(prev_df: DataFrame, segment_name: str):
    segment_df = create_segment_df(segment_name)
    segment_df = segment_df.withColumn(SEGMENT, f.lit(segment_name)).select("segment", *segment_df.columns)
    return prev_df.unionByName(segment_df, allowMissingColumns=True)


def create_segments_union_df(segments_config: Dict[str, Any]) -> DataFrame:
    spark = SparkSession.getActiveSession()
    empty_df = spark.createDataFrame([], t.StructType([]))
    return reduce(union_segments, segments_config.keys(), empty_df)

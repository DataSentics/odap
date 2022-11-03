from datetime import datetime
from typing import Dict
from pyspark.sql import DataFrame
from odap.common.databricks import get_workspace_api
from odap.common.dataframes import create_dataframe, create_dataframe_from_notebook_cells
from odap.common.utils import get_absolute_path, get_notebook_cells
from odap.segment_factory.config import get_segment_table, get_segment_table_path
from odap.segment_factory.exceptions import SegmentNotFoundException
from odap.segment_factory.schemas import get_segment_common_fields_schema


def write_segment(
    df: DataFrame,
    segment_name: str,
    export_id: str,
    export_name: str,
    timestamp: datetime,
    segment_factory_config: Dict,
):
    extended_segment_df = create_dataframe(
        [[export_id, timestamp, export_name]], get_segment_common_fields_schema()
    ).join(df, how="full")

    table = get_segment_table(segment_name, segment_factory_config)
    (
        extended_segment_df.write.format("delta")
        .mode("append")
        .option("path", get_segment_table_path(segment_name, segment_factory_config))
        .saveAsTable(table)
    )
    return table


def create_segment_df(segment_name: str) -> DataFrame:
    workspace_api = get_workspace_api()

    segment_path = get_absolute_path("segments", segment_name)

    notebook_cells = get_notebook_cells(segment_path, workspace_api)
    segment_df = create_dataframe_from_notebook_cells(segment_path, notebook_cells)

    if not segment_df:
        raise SegmentNotFoundException(f"Segment '{segment_name}' could not be loaded")
    return segment_df
